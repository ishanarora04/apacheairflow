import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class RedshiftVacuumOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_connection_id, query , *args, **kwargs):
        self.redshift_connection_id = redshift_connection_id;
        self.query = query;
        super(RedshiftVacuumOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = PostgresHook(self.redshift_connection_id);
        connection = hook.get_conn();
        cursor = connection.cursor();
        cursor.execute(self.query);
        cursor.close();
        connection.close();
        logging.info("The query is executed" + self.query);
        return True;

class RedshiftVacuumPlugin(AirflowPlugin):
    name = "redshift_vacumm_plugin";
    operators = [RedshiftVacuumOperator];