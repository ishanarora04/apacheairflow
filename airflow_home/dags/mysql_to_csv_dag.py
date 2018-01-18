
from datetime import datetime
from airflow import DAG
from airflow.operators import MySQLToCSVOperator
from airflow.operators.dummy_operator import DummyOperator;

args = {
    'owner': 'scopeworker',
    'provide_context': True
}

dag = DAG('mysql_to_csv_dag', description='Another tutorial DAG',
          schedule_interval="@once",
          start_date=datetime(2017, 3, 20), catchup=False,
          default_args=args);

mysql_operator = MySQLToCSVOperator(task_id="mysql_to_csv",
                                    mysql_conn_id="mysql_localhost",
                                    sql="select * from employee" ,
                                    schema_filename="employee",
                                    filename = "employee_file",
                                    aws_conn_id = "s3_kato",
                                    s3_bucket_name = "scopeworkerkato",
                                    redshift_connection_id="redshift_scopeworker_dev",
                                    dag=dag);

dummy_task = DummyOperator(task_id='dummy_task',
                           dag=dag);

dummy_task >> mysql_operator;