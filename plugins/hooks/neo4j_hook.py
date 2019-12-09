from airflow.hooks.base_hook import BaseHook
from neo4j import GraphDatabase
import logging

logger = logging.getLogger(__name__)

class Neo4jHook(BaseHook):
    def __init__(self,neo4j_read_conn_id='neo4j_read_default',
                 neo4j_write_conn_id='neo4j_write_default'):
        self.neo4j_read_conn_id = neo4j_read_conn_id
        self.neo4j_write_conn_id = neo4j_write_conn_id

    def _get_driver(self,conn_id):
        if conn_id:
            conn = self.get_connection(conn_id)
        return GraphDatabase.driver(conn.host, auth=(conn.login, conn.password))

    def get_read_driver(self):
        return self._get_driver(self.neo4j_read_conn_id)

    def get_write_driver(self):
        return self._get_driver(self.neo4j_write_conn_id)

