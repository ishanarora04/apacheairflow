from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
import logging
from airflow import configuration


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

    create_new_conn(session,
                    {"conn_id": configuration.get('s3' , 's3_conn_id'),
                     "conn_type": configuration.get('s3' , 's3_conn_type'),
                     "extra":configuration.get('s3', 's3_extra')

                     })

    create_new_conn(session,
                    {"conn_id": configuration.get('mysql', 'mysql_conn_id'),
                     "conn_type": configuration.get('mysql', 'mysql_conn_type'),
                     "schema":configuration.get('mysql', 'mysql_schema'),
                     "host": configuration.get('mysql', 'mysql_host'),
                     "port": configuration.getint('mysql', 'mysql_port'),
                     "login": configuration.get('mysql', 'mysql_login'),
                     "password": configuration.get('mysql', 'mysql_password')})

    create_new_conn(session,
                    {"conn_id": configuration.get('postgresql', 'postgresql_conn_id'),
                     "conn_type": configuration.get('postgresql', 'postgresql_conn_type'),
                     "host": configuration.get('postgresql', 'postgresql_host'),
                     "port": configuration.getint('postgresql', 'postgresql_port'),
                     "schema": configuration.get('postgresql', 'postgresql_schema'),
                     "login": configuration.get('postgresql', 'postgresql_login'),
                     "password": configuration.get('postgresql', 'postgresql_password')})

    create_new_conn(session,
                    {"conn_id": "airflow_connection",
                     "conn_type": configuration.get('mysql', 'mysql_conn_type'),
                     "schema": "airflow",
                     "host": "localhost",
                     "login": "airflow",
                     "password": "airflow"})

    session.close()

dag = airflow.DAG(
    'default_connection',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='default_conn',
                    python_callable=auto_conn,
                    provide_context=False,
                    dag=dag)