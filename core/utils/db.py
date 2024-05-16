"""
Utility functions to manipulate connections
to databases.
"""
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.connection import Connection
from datetime import datetime
from core.utils.data import query_to_csv


def perform_query(conn: Connection,
                  query: str,
                  output_type: str,
                  save_path: Path):
    """Runs a query to a specific db connected
    and saves it to the desired path and type."""
    cursor = conn.cursor()
    queryresult = cursor.execute(query)

    if output_type == 'csv':
        query_to_csv(cursor=cursor, save_path=save_path)

    cursor.close()
    return queryresult


def connect_to_testdb() -> Connection:
    """Returns a connection with a test db."""
    hook = PostgresHook(postgres_conn_id="test_localhost")
    conn = hook.get_conn()
    print(conn)
    return conn


def close_connection(conn: Connection) -> None:
    try:
        conn.close()
    except Exception as e:
        print(f'Error closing a connection: {e}')
