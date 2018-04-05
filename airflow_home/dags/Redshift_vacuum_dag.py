from airflow import DAG
from airflow.operators import RedshiftVacuumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import configuration;
from datetime import datetime

args = {
    'owner': 'scopeworker',
    'provide_context': True
}

dag = DAG('redshift_vacuum_plugin', description='REDSHIFT VACUUM DAG',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2017, 3, 20),
          catchup=False,
          default_args=args)


redshift_operator = RedshiftVacuumOperator(task_id="vacumming_task",
                                        redshift_connection_id=configuration.get("postgresql", "postgresql_conn_id"),
                                         query = "COMMIT;vacuum; COMMIT;",
                                         dag=dag)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

dummy_operator >> redshift_operator