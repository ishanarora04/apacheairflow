import logging

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from mongo_hook import MongoHook


class MongoToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(self, mongo_conn_id,  *args, **kwargs):
        self.mongo_conn_id = mongo_conn_id
        super(MongoToS3Operator, self).__init__(*args, **kwargs)

    def execute(self, context):
        mongo_hook = MongoHook(self.mongo_conn_id)
        connection = mongo_hook.get_conn()
        print(connection['admin'])
        return True


class MongoToS3Plugin(AirflowPlugin):

    name = "MongoToS3Plugin"
    operators = [MongoToS3Operator]

