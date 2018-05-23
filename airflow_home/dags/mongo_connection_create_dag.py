from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
import logging


args = {
    'owner': 'scopeworker',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}



def auto_conn():
    logging.info('Creating connections, pool and sql path')

    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.extra = attributes.get('extra')
        # new_conn.password = attributes.get('password')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()

    create_new_conn(session, {
        "conn_id": "mongo_connection",
        "conn_type": "mongo",
        "host": "13.126.117.239",
        "port": "27017",
        "login": "mongo",
        "password": "password"
    });

    session.close();

dag = airflow.DAG(
    'mongo_connection',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='mongo_connection',
                    python_callable=auto_conn,
                    provide_context=False,
                    dag=dag)