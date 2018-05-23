from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator;
from airflow.operators import MongoMetadataOperator
from datetime import  datetime;
import airflow;
from airflow import configuration;

args = {
    'owner': 'scopeworker',
    'provide_context': True,
    'start_date' : airflow.utils.dates.days_ago(7),
}

dag = DAG('mongo_collections_to_mysql', description='Tables to Mysql',
          schedule_interval="0 */5 * * *",
          start_date=datetime(2017, 3, 20), catchup=False,
          default_args=args);

start_task = DummyOperator(task_id='dummy_task', dag=dag);

python_task = MongoMetadataOperator(task_id="inserting_collections", mongo_conn_id = "mongo_connection" , dag=dag);

complete_task = DummyOperator(task_id="complete_task", dag=dag);

start_task >> python_task >> complete_task;