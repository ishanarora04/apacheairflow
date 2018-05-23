from airflow.hooks.dbapi_hook import DbApiHook
import pymongo


class MongoHook(DbApiHook):

    conn_name_attr = 'mongo_conn_id'
    default_conn_name = 'mongo_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MongoHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        conn = self.get_connection(self.mongo_conn_id)
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or '',
            "host": conn.host or 'localhost',
            "db": conn.schema or ''
        }
        if not conn.port:
            conn_config["port"] = 27017
        else:
            conn_config["port"] = int(conn.port)
        conn_str = "mongodb://{0}:{1}@{2}:{3}".format(conn_config['user'], conn_config['passwd'], conn_config['host'], conn_config['port'])
        client = pymongo.MongoClient(conn_str)
        return client
