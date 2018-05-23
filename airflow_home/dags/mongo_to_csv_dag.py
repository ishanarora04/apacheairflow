from datetime import datetime
from airflow import DAG
from airflow.operators import MongoToS3Operator
from airflow.operators.dummy_operator import DummyOperator;

args = {
    'owner': 'scopeworker',
    'provide_context': True,
}

dag = DAG('mongo_to_csv_dag', description='Another tutorial DAG',
          schedule_interval="*/1 * * * *",
          start_date=datetime(2017, 3, 20), catchup=False,
          default_args=args);

dummy_task = DummyOperator(task_id='dummy_task',
                           dag=dag);
complete = DummyOperator(task_id='task_complete', dag=dag);

mongo_csv_operator =  MongoToS3Operator(task_id="mongo_to_csv_task",
                                    mongo_conn_id = "mongo_connection" ,
                                    dag=dag);

dummy_task >> mongo_csv_operator >> complete;