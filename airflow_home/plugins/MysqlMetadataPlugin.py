
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook

class MysqlMetadataOperator(BaseOperator):
    @apply_defaults
    def __init__(self,mysql_conn_id,
                 *args, **kwargs):

        self.mysql_conn_id = mysql_conn_id;
        super(MysqlMetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.mysql_metadata_hook = MySqlHook("airflow_connection");
        self.mysql_table_hook = MySqlHook(self.mysql_conn_id);
        self.insert_tables();
        return True;

    def insert_tables(self):

        database = self.mysql_conn_id;

        tables = [];

        db = self.mysql_metadata_hook.get_conn();

        generator = self.mysql_statement_generator();

        mysql_main_table = generator.next();
        self.mysql_statement_executor(db, mysql_main_table);

        mysql_main_staging_table = generator.next();
        self.mysql_statement_executor(db, mysql_main_staging_table);

        mysql_show_tables = generator.next();
        for table in self.mysql_statement_executor(self.mysql_table_hook.get_conn(), mysql_show_tables):
            tables.append((database, table[0], 1));

        delete_metadata = generator.next();
        self.mysql_statement_executor(db, delete_metadata);

        insert_metadata = generator.next();
        insert_metadata = insert_metadata + ",".join("(%s,%s ,%s)" for _ in tables);
        flattened_values = [item for sublist in tables for item in sublist]
        self.mysql_statement_executor(db, insert_metadata, flattened_values);

        delete_data_main_table = generator.next();
        self.mysql_statement_executor(db, delete_data_main_table);

        insert_data_main_table = generator.next();
        self.mysql_statement_executor(db, insert_data_main_table);

        db.commit();
        db.close();

        return 0;

    def mysql_statement_generator(self):

        yield "CREATE TABLE IF NOT EXISTS mysql_metadata (id INT(10) AUTO_INCREMENT, db_name VARCHAR(50), table_name VARCHAR(128) , is_active TINYINT(4) , created_at TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP , PRIMARY KEY(id) ) ";

        yield "CREATE TABLE IF NOT EXISTS mysql_metadata_staging (id INT(10) AUTO_INCREMENT, db_name VARCHAR(50), table_name VARCHAR(128) , is_active TINYINT(4) , created_at TIMESTAMP NOT NULL  DEFAULT CURRENT_TIMESTAMP,  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,PRIMARY KEY(id)) ";

        yield "SHOW tables";

        yield "DELETE FROM mysql_metadata_staging";

        yield "INSERT INTO mysql_metadata_staging (db_name, table_name, is_active) VALUES ";

        yield "DELETE FROM mysql_metadata";

        yield "INSERT INTO mysql_metadata SELECT * FROM mysql_metadata_staging";

        return;

    def mysql_statement_executor(self , db,  mysql, values=None):
        cursor = db.cursor();
        if values is None:
            cursor.execute(mysql);
        else:
            cursor.execute(mysql, values);
        result = cursor.fetchall();
        cursor.close();
        return result;


class MysqlMetadataPlugin(AirflowPlugin):
    name = "mysql_metadata_plugin"
    operators = [MysqlMetadataOperator]

