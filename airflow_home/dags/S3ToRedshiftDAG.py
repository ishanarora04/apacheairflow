

from datetime import datetime
from airflow import DAG
from airflow.operators import S3ToRedshiftOperator
from airflow.operators.dummy_operator import DummyOperator;


args = {
    'owner': 'scopeworker',
    'provide_context': True
}

dag = DAG('s3_to_redshift_dag', description='REDSHIFT DAG',
          schedule_interval='*/4 * * * *',
          start_date=datetime(2017, 3, 20),
          catchup=False,
          default_args=args);


redshift_operator = S3ToRedshiftOperator(task_id="s3_to_redshift",
                                         redshift_conn_id="redshift_scopeworker_dev",
                                         s3_bucket="scopeworkerkato",
                                         s3_access_key_id = 'AKIAJ74FA6IKIFH3M7GA',
                                         s3_secret_access_key = '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',
                                         delimiter = ",",
                                         region = "us-east-1",
                                         aws_conn_id = "s3_kato",
                                         dag=dag);

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag);

dummy_operator >> redshift_operator



