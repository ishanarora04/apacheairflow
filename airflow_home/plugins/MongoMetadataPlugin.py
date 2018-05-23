
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook
from mongo_hook import MongoHook


class MongoMetadataOperator(BaseOperator):

    @apply_defaults
    def __init__(self, mongo_conn_id, *args, **kwargs):
        self.mongo_conn_id = mongo_conn_id
        super(MongoMetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        self.mongo_hook = MongoHook(self.mongo_conn_id)
        self.mysql_metadata_hook = MySqlHook("airflow_connection")
        self.insert_tables()
        return True

    def insert_tables(self):

        mongo_conn = self.mongo_hook.get_conn()
        db = mongo_conn['sw_client']
        collections = db.collection_names(include_system_collections=False)
        mongo_conn.close()

        tables = []
        db = self.mysql_metadata_hook.get_conn()
        generator = self.mysql_statement_generator()

        mysql_main_table = generator.next()
        self.mysql_statement_executor(db, mysql_main_table)

        mysql_main_staging_table = generator.next()
        self.mysql_statement_executor(db, mysql_main_staging_table)

        for table in collections:
            tables.append(('sw_client', table, 1))

        delete_metadata = generator.next()
        self.mysql_statement_executor(db, delete_metadata)

        insert_metadata = generator.next()
        insert_metadata = insert_metadata + ",".join("(%s,%s ,%s)" for _ in tables)
        flattened_values = [item for sublist in tables for item in sublist]
        self.mysql_statement_executor(db, insert_metadata, flattened_values)

        delete_data_main_table = generator.next()
        self.mysql_statement_executor(db, delete_data_main_table)

        insert_data_main_table = generator.next()
        self.mysql_statement_executor(db, insert_data_main_table)

        db.commit()
        db.close()

        return 0


    def mysql_statement_generator(self):

        yield '''CREATE TABLE IF NOT EXISTS mongo_metadata (id INT(10) AUTO_INCREMENT, db_name VARCHAR(50), 
            table_name VARCHAR(128) , is_active TINYINT(4) , created_at TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,  
             updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP , PRIMARY KEY(id) ) '''

        yield ''' CREATE TABLE IF NOT EXISTS mongo_metadata_staging (id INT(10) AUTO_INCREMENT, db_name VARCHAR(50), 
            table_name VARCHAR(128) , is_active TINYINT(4) , created_at TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,  
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,PRIMARY KEY(id)) '''

        yield "DELETE FROM mongo_metadata_staging"

        yield "INSERT INTO mongo_metadata_staging (db_name, table_name, is_active) VALUES "

        yield "DELETE FROM mongo_metadata"

        yield "INSERT INTO mongo_metadata SELECT * FROM mysql_metadata_staging"

        return

    def mysql_statement_executor(self , db,  mysql, values=None):
        cursor = db.cursor()
        if values is None:
            cursor.execute(mysql)
        else:
            cursor.execute(mysql, values)
        result = cursor.fetchall()
        cursor.close()
        return result


class MongoMetadataPlugin(AirflowPlugin):

    name = "MongoToS3Plugin"
    operators = [MongoMetadataOperator]