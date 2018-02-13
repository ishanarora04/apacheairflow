import logging
import os
import pandas as pd
import numpy as np

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook;
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from MySQLdb.constants import FIELD_TYPE
from airflow.hooks.postgres_hook import PostgresHook

log = logging.getLogger(__name__)

class MySQLToCSVOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 redshift_connection_id,
                 aws_conn_id ,
                 s3_bucket,
                 approx_max_file_size_bytes = 19000000,
                 *args, **kwargs):

        self.mysql_conn_id = mysql_conn_id;
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.aws_conn_id = aws_conn_id;
        self.s3_bucket_name = s3_bucket;
        self.redshift_connection_id = redshift_connection_id;

        super(MySQLToCSVOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.mysql_hook = MySqlHook(self.mysql_conn_id);
        self.mysql_metadata_hook = MySqlHook("airflow_connection");
        self.table_name = self.fetch_table_name();

        if self.table_name is None:
            return True;

        self.query_process(self.table_name);
        return True;

    def query_process(self , data):

        table_data = data[0];
        table_name = table_data[2];
        cursor = self._query_mysql(hook = self.mysql_hook, mysql_query="select * from " + table_name + " limit 1" );
        files_to_upload = self._write_local_data_files(cursor , table_name);

        fetch_columns_redshift , count_field_redshift = self._fetch_field_names_table(table_name);

        # Creating table in Staging
        redshift_statement_staging, redshift_statement_main = self._create_table_statement(cursor , table_name);
        self._redshift_staging_table(redshift_sql=redshift_statement_main + ";" + redshift_statement_staging);

        #Alter table Statement
        if count_field_redshift > 0 and  count_field_redshift != len(cursor.description):
            redshift_field = frozenset([field[0] for field in fetch_columns_redshift]);
            mysql_fields = frozenset(self._write_local_file_schema(cursor));
            difference = (mysql_fields.difference(redshift_field));
            alter_fields = [];
            for elem in cursor.description:
                if elem[0] in difference:
                    alter_fields.append(elem);
            redshift_alter_table_commands = self._alter_table(table_name, alter_fields);
            self._redshift_staging_table(";".join(redshift_alter_table_commands));

        self._upload_files(files_to_upload);
        cursor.close();

        return True;


    def _alter_table(self, table_name , alter_fields):

        redshift_sql_main = "alter table " +  "" + table_name + " add column ";
        redshift_sql_staging = "alter table " + " " + table_name + "_staging" + " add column ";
        redshift_alter_fields = [];
        for mysql_fields in alter_fields:
            redshift_alter_fields.append((redshift_sql_main + mysql_fields[0]) + " " + self.type_map(mysql_fields[1]));
            redshift_alter_fields.append((redshift_sql_staging + mysql_fields[0]) + " " + self.type_map(mysql_fields[1]));
        return redshift_alter_fields ;


    def fetch_table_name(self):

        table_name_query = "select * from mysql_metadata where is_active = 1 and db_name = '" + self.mysql_conn_id + "' limit 1";
        cursor = self._query_mysql(hook = self.mysql_metadata_hook, mysql_query=table_name_query );
        result = cursor.fetchall();
        cursor.close()

        if len(list(result)) == 0:
            return None;

        logging.info("The table name under process is {}".format(result));
        update_data = 'update mysql_metadata set is_active = 0 where id in (';
        update_data += ",".join("%s" for _ in result);
        update_data += ")";
        results = [row[0] for row in result];
        log.info("The Update statement fired {}" + update_data);
        self._query_mysql(hook = self.mysql_metadata_hook,mysql_query=update_data, value=results);
        cursor.close();
        return result;



    def _query_mysql(self, hook, mysql_query, value=None):
        """
               Queries mysql and returns a cursor to the results.
        """
        connection = hook.get_conn();
        cursor = connection.cursor();
        if value is None:
            cursor.execute(mysql_query);
        else:
            cursor.execute(mysql_query, value);
        logging.info("cursor Executed statement : {}".format(mysql_query));
        connection.commit();
        return cursor;


    def _create_csv_file(self, mysql_query, csv):

        df = pd.read_sql_query(mysql_query, self.mysql_hook.get_conn());
        df = df.replace('\n', '', regex=True)
        df.to_csv(path_or_buf=csv.name, sep = '\t', encoding='utf-8', index=False);
        return csv


    def _write_local_data_files(self, cursor, table_name):
        """
            Cursor.description provides the metadata about the cursor
            Takes a cursor and iterates over it .
            Prints the cursor data
        """
        primary_key = cursor.description[cursor.lastrowid][0];
        file_key = "m&{0}&{1}.csv".format(table_name, primary_key);
        file_handle = open(file_key, 'w')
        file_handle = self._create_csv_file(mysql_query="select * from " + table_name, csv = file_handle);
        return {file_key : file_handle};

    def _write_local_file_schema(self, cursor):
        """
            Takes a cursor and Writes the schema to a temporary file ...
            A view towards Redshift
        """
        fields = [];
        for field in cursor.description:
            field_name = field[0];
            fields.append(field_name);
        return fields;

    def _upload_files(self, files):

        dest_s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        if dest_s3 is None:
            raise AirflowException("Unable to connect to the S3 Bucket");

        if not dest_s3.check_for_bucket(self.s3_bucket_name):
            raise AirflowException("Could not find the bucket {0}".format(self.s3_bucket_name));

        for key, file_handle in files.items():
            if os.path.exists(file_handle.name):
                dest_s3.load_file(bucket_name=self.s3_bucket_name, filename=file_handle.name, key = key , replace=True);
            else:
                raise AirflowException("File Not Found");

        for key , file_handle in files.items():
            if os.path.exists(file_handle.name):
                os.remove(file_handle.name);

        return True;


    def _create_table_statement(self, cursor, table_name):

        redshift_statement = "CREATE TABLE IF NOT EXISTS " ;

        redshift_fields = [];
        primary_key = (cursor.description[cursor.lastrowid]);

        #TODO : $ field in Mongo Tables

        for mysql_fields in cursor.description:
            redshift_fields.append((mysql_fields[0]) + " " + self.type_map(mysql_fields[1]));
        redshift_fields.append("PRIMARY KEY({0})".format(primary_key[0]));
        redshift_fields = ",".join(redshift_fields);
        redshift_statement_staging = redshift_statement  + "" + table_name + "_staging (" + redshift_fields + ")";
        redshift_statement_main = redshift_statement  + "" + table_name  + "(" +  redshift_fields + ")"

        return redshift_statement_staging ,redshift_statement_main;


    def _redshift_staging_table(self, redshift_sql):

        hook = PostgresHook(self.redshift_connection_id);
        connection = hook.get_conn();
        cursor = connection.cursor();
        redshift_sql  = "begin transaction; " + redshift_sql;
        redshift_sql += ";end transaction;";
        cursor.execute(redshift_sql);
        cursor.close();
        return True;


    def _fetch_field_names_table(self, table_name):

        table_columns = "select \"column\" ,type from pg_table_def where tablename = %s"  ;
        hook = PostgresHook(self.redshift_connection_id);
        connection = hook.get_conn();
        cursor = connection.cursor();
        redshift_sql = table_columns;
        cursor.execute(redshift_sql, [table_name]);
        columns = cursor.fetchall();
        cursor.close()
        return columns , len(columns);


    @classmethod
    def type_map(cls, mysql_type):
            """
            Helper function that maps from MySQL fields to Redshift fields. Used
            when a schema_filename is set.
            """
            d = {
                FIELD_TYPE.INT24: 'DECIMAL',
                FIELD_TYPE.TINY: 'DECIMAL',
                FIELD_TYPE.BIT: 'DECIMAL',
                FIELD_TYPE.DATETIME: 'TIMESTAMP',
                FIELD_TYPE.DATE: 'DATE',
                FIELD_TYPE.DECIMAL: 'FLOAT',
                FIELD_TYPE.NEWDECIMAL: 'FLOAT',
                FIELD_TYPE.DOUBLE: 'FLOAT',
                FIELD_TYPE.FLOAT: 'FLOAT',
                FIELD_TYPE.LONG: 'DECIMAL',
                FIELD_TYPE.LONGLONG: 'DECIMAL',
                FIELD_TYPE.SHORT: 'INTEGER',
                FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
                FIELD_TYPE.YEAR: 'INTEGER',
            }
            return d[mysql_type] if mysql_type in d else 'VARCHAR(max)';



class MysqlToCSVPlugin(AirflowPlugin):
    name = "mysql_to_csv_plugin";
    operators = [MySQLToCSVOperator];
