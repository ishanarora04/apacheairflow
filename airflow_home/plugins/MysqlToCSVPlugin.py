import csv;
import logging
import os

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook;
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from MySQLdb.constants import FIELD_TYPE
from collections import namedtuple
from airflow.hooks.postgres_hook import PostgresHook

log = logging.getLogger(__name__)

Column = namedtuple("table_record", 'field_name, field_type , field_mode');


class MySQLToCSVOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 redshift_connection_id,
                 sql,
                 schema_filename,
                 filename,
                 aws_conn_id ,
                 s3_bucket_name,
                 approx_max_file_size_bytes = 19000000,
                 *args, **kwargs):

        self.mysql_conn_id = mysql_conn_id;
        self.sql = sql;
        self.schema_filename = schema_filename;
        self.filename = filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.aws_conn_id = aws_conn_id;
        self.s3_bucket_name = s3_bucket_name;
        self.table_name = "employee";
        self.redshift_connection_id = redshift_connection_id;

        super(MySQLToCSVOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        tables_present = 'SHOW TABLES';
        cursor_tables_present = self._query_mysql(tables_present);

        for data in cursor_tables_present:
            self.query_process(data);

        return True;


    def query_process(self , data):

        table_name = data[0];
        cursor = self._query_mysql("select * from " + table_name);
        files_to_upload = self._write_local_data_files(cursor , table_name);

        # Creating table in Staging
        redshift_statement_staging, redshift_statement_main = self._create_table_statement(cursor , table_name);
        self._redshift_staging_table(redshift_sql=redshift_statement_staging);
        self._redshift_staging_table(redshift_sql=redshift_statement_main);

        self._upload_files(files_to_upload);

        cursor.close();

        return True;

    def _query_mysql(self, mysql_query):
        """
               Queries mysql and returns a cursor to the results.
        """
        hook = MySqlHook(self.mysql_conn_id);
        connection = hook.get_conn();
        cursor = connection.cursor();
        cursor.execute(mysql_query);
        return cursor;

    def _write_local_data_files(self, cursor, table_name):
        """
            Cursor.description provides the metadata about the cursor
            Takes a cursor and iterates over it .
            Prints the cursor data
        """
        primary_key = cursor.description[cursor.lastrowid][0];
        file_key = "m&{0}&{1}.csv".format(table_name, primary_key);
        file_handle = open(file_key , 'w');
        with file_handle:
            field_names = self._write_local_file_schema(cursor);
            writer = csv.DictWriter(file_handle, fieldnames=field_names);
            writer.writeheader()
            for row in cursor:
                row_dict = dict(zip(field_names, row))
                writer.writerow(row_dict);
        print(file_key);
        return {file_key : file_handle};

    def _write_local_file_schema(self, cursor):
        """
            Takes a cursor and Writes the schema to a temporary file ...
            A view towards Redshift
        """
        fields = [];
        for field in cursor.description:
            print(field);
            field_name = field[0];
            fields.append(field_name);
        return fields;

    def _upload_files(self, files):

        dest_s3 = S3Hook(aws_conn_id=self.aws_conn_id)

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
        #+ self.table_name + "(";
        redshift_fields = [];
        primary_key = (cursor.description[cursor.lastrowid]);
        for mysql_fields in cursor.description:
            redshift_fields.append(mysql_fields[0] + " " + self.type_map(mysql_fields[1]));
        redshift_fields.append("PRIMARY KEY({0})".format(primary_key[0]));
        redshift_fields = ",".join(redshift_fields);
        redshift_statement_staging = redshift_statement + table_name + "_staging (" + redshift_fields + ")";
        redshift_statement_main = redshift_statement + table_name  + "(" +  redshift_fields + ")"

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

    @classmethod
    def type_map(cls, mysql_type):
            """
            Helper function that maps from MySQL fields to Redshift fields. Used
            when a schema_filename is set.
            """
            d = {
                FIELD_TYPE.INT24: 'INTEGER',
                FIELD_TYPE.TINY: 'INTEGER',
                FIELD_TYPE.BIT: 'INTEGER',
                FIELD_TYPE.DATETIME: 'TIMESTAMP',
                FIELD_TYPE.DATE: 'DATE',
                FIELD_TYPE.DECIMAL: 'FLOAT',
                FIELD_TYPE.NEWDECIMAL: 'FLOAT',
                FIELD_TYPE.DOUBLE: 'FLOAT',
                FIELD_TYPE.FLOAT: 'FLOAT',
                FIELD_TYPE.LONG: 'INTEGER',
                FIELD_TYPE.LONGLONG: 'INTEGER',
                FIELD_TYPE.SHORT: 'INTEGER',
                FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
                FIELD_TYPE.YEAR: 'INTEGER',
            }
            return d[mysql_type] if mysql_type in d else 'VARCHAR';



class MysqlToCSVPlugin(AirflowPlugin):
    name = "mysql_to_csv_plugin";
    operators = [MySQLToCSVOperator];
