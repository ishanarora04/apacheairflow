import logging
import random

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import  S3Hook;

log = logging.getLogger(__name__)

class S3ToRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 s3_bucket,
                 s3_access_key_id,
                 s3_secret_access_key,
                 delimiter,
                 region,
                 aws_conn_id,
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.delimiter = delimiter
        self.region = region
        self.aws_conn_id = aws_conn_id;
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        #Fetching Existing Files
        if not self._fetch_file_names():
            return True;

        #Upserting Redshift
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        conn = self.hook.get_conn()
        cursor = conn.cursor()
        log.info("Connected with " + self.redshift_conn_id)
        data_upload = self._upload_data_to_staging(cursor);
        if data_upload:
            cursor = conn.cursor();
            self._upsertData(cursor);
        conn.commit()

        #Deleting corresponding files from S3 in order to process Later
        delete_file = {
            'Objects': [
                {
                    'Key': self.s3_path,
                },
            ],
            'Quiet': True | False
        }
        self.bucket.delete_objects(Delete=delete_file);

        return True

    def _fetch_file_names(self):

        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id);

        self.bucket = self.s3_hook.get_bucket(self.s3_bucket);
        if not self.bucket:
            raise AirflowException("Bucket Does Not Exist");

        s3_keys = self.s3_hook.list_keys(bucket_name=self.s3_bucket , prefix="m&");

        if s3_keys is not None and len(s3_keys) > 0:
            self.s3_path = s3_keys[random.randint(0,len(s3_keys)-1)];
            key_breaks = self.s3_path.split(".");
            index_files = key_breaks[0].split("&");
            table_name = index_files[1];
            primary_key = index_files[2];
            self.src_table = table_name + "_staging";
            self.dest_table = table_name;
            self.src_keys = [primary_key];
            self.dest_keys = [primary_key];
            return True;

        return False;

    def _upload_data_to_staging(self, cursor):


        load_statement = """
                        delete from {0};
                        copy
                        {0}
                        from 's3://{1}/{2}'
                        access_key_id '{3}' secret_access_key '{4}'
                        delimiter '{5}' region '{6}' DATEFORMAT 'auto' TIMEFORMAT 'auto' ACCEPTINVCHARS EMPTYASNULL BLANKSASNULL FILLRECORD IGNOREHEADER 1 """.format(
                        self.src_table, self.s3_bucket, self.s3_path,
                        self.s3_access_key_id, self.s3_secret_access_key,
                        self.delimiter, self.region);
        log.info("The Load statement is {}".format(load_statement));
        cursor.execute(load_statement)
        log.info("The Load statement is executed");
        cursor.close()
        log.info("Load command completed");

        return True;

    def _upsertData(self, cursor):

        # build the SQL statement
        sql_statement = "begin transaction; "
        sql_statement += "delete from "  + self.dest_table + " using " + self.src_table + " where "
        for i in range(0, len(self.src_keys)):
            sql_statement +=  self.src_table + "." + self.src_keys[i] + " = " +  self.dest_table + "." + self.dest_keys[i]
            if (i < len(self.src_keys) - 1):
                sql_statement += " and "

        sql_statement += "; "
        sql_statement += " insert into "  + self.dest_table + " select * from " +  self.src_table + " ; "
        sql_statement += " end transaction; "
        cursor.execute(sql_statement)
        cursor.close()

class S3ToRedshiftOperatorPlugin(AirflowPlugin):
  name = "redshift_load_plugin"
  operators = [S3ToRedshiftOperator]