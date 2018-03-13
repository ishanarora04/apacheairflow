from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class RedshiftErrorSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, redshift_connection_id, *args, **kwargs):
        self.redshift_connection_id = redshift_connection_id
        super(RedshiftErrorSensor, self).__init__(*args, **kwargs)

    def poke(self, context):

        hook = PostgresHook(self.redshift_connection_id);
        connection = hook.get_conn();


        return True;




