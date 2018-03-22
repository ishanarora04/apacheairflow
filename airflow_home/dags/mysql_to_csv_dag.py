from datetime import datetime
from airflow import DAG
from airflow.operators import MySQLToCSVOperator
from airflow.operators.dummy_operator import DummyOperator;
from airflow import configuration;

args = {
    'owner': 'scopeworker',
    'provide_context': True,
}

dag = DAG('mysql_to_csv_dag', description='Another tutorial DAG',
          schedule_interval="*/1 * * * *",
          start_date=datetime(2017, 3, 20), catchup=False,
          default_args=args);

dummy_task = DummyOperator(task_id='dummy_task',
                           dag=dag);
complete = DummyOperator(task_id='task_complete', dag=dag);

mysql_csv_operator =  MySQLToCSVOperator(task_id="mysql_to_csv_task",
                                    mysql_conn_id= configuration.get("mysql", "mysql_conn_id"),
                                    aws_conn_id = configuration.get("s3", "s3_conn_id"),
                                    s3_bucket = "scopeworkerproduction",
                                    redshift_connection_id=configuration.get("postgresql", "postgresql_conn_id"),
                                    dag=dag);

dummy_task >> mysql_csv_operator >> complete;