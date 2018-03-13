

from datetime import datetime
from airflow import DAG
from airflow.operators import S3ToRedshiftOperator
from airflow.operators.dummy_operator import DummyOperator;
from airflow import configuration;

args = {
    'owner': 'scopeworker',
    'provide_context': True
}

dag = DAG('s3_to_redshift_dag', description='REDSHIFT DAG',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2017, 3, 20),
          catchup=False,
          default_args=args);


redshift_operator = S3ToRedshiftOperator(task_id="s3_to_redshift",
                                         redshift_conn_id=configuration.get("postgresql", "postgresql_conn_id"),
                                         s3_bucket="scopeworkerkato",
                                         aws_conn_id=configuration.get("s3", "s3_conn_id"),
                                         s3_access_key_id = configuration.get("s3", "s3_access_key_id"),
                                         s3_secret_access_key = configuration.get("s3", "s3_secret_access_key"),
                                         delimiter = '|',
                                         region = "us-east-1",
                                         dag=dag);

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag);

dummy_operator >> redshift_operator






