"""
Utility functions to manipulate connections
to databases.
"""
import logging

from airflow.models import connection
from airflow.providers.postgres.hooks.postgres import PostgresHook


def perform_query(*args,
                  conn: connection,
                  query: str,
                  many=True):
    """Performs a query to a specific db.

    :param conn: The connection to a db.
    :param query: The SQL query to execute.
    :param args: Values to replace placeholders for the query.
    :param many: Set to false if you expect one entry returned from the query.

    :returns: The result of the query."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, args)

            if many:
                queryresult = cursor.fetchall()
            else:
                queryresult = cursor.fetchone()
            return queryresult
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise
    finally:
        cursor.close()


def connect_to_eae() -> connection:
    """Returns a connection with eae database."""
    hook = PostgresHook(postgres_conn_id="eae_vpn")
    try:
        conn = hook.get_conn()
        return conn
    except ConnectionRefusedError as e:
        logging.error(f'Connection Refused: {e}')
        raise

from airflow.models.connection import Connection
from datetime import datetime


def perform_query(*args,
                  conn: Connection,
                  query: str) -> list:
    """Runs a query to a specific db.

    :argument args: additional parameters for the query
    :argument conn: the connection object to the specific db
    :argument query: query to execute

    :returns: the result of the query executed
    """
    cursor = conn.cursor()

    try:

        cursor.execute(query, args)

        queryresult = cursor.fetchall()

        return queryresult

    except Exception as e:
        print(e)
    finally:
        cursor.close()


def connect_to_postgres(database: str) -> Connection:
    """Establish a connection with a postgres database.
    :returns: A connection object.
    """
    hook = PostgresHook(postgres_conn_id=database)
    try:
        conn = hook.get_conn()
        return conn

    except ConnectionRefusedError as e:
        print(e)
    except ConnectionError as e:
        print(e)


def connect_to_testdb() -> Connection:
    """Establish a connection with a test db.
    :returns: A connection object.
    """
    hook = PostgresHook(postgres_conn_id="test_localhost")
    try:
        conn = hook.get_conn()
        return conn

    except ConnectionRefusedError as e:
        print(e)
    except ConnectionError as e:
        print(e)


def connect_to_testdb() -> connection:
    """Returns a connection with a test db.
    Edit connection credentials from the airflow connection tab in the UI."""
    hook = PostgresHook(postgres_conn_id="test_localhost")
    try:
        conn = hook.get_conn()
        return conn
    except ConnectionRefusedError as e:
        logging.error(f'Connection Refused: {e}')
        raise
    except ConnectionError as e:
        print(e)


def close_connection(conn: connection) -> None:
    """Close a connection.
    :param conn: The connection you want to close.
    """

def close_connection(conn: Connection) -> None:
    """Close a connection"""
    try:
        conn.close()
    except ConnectionError as e:
        logging.error(f'Error closing a connection: {e}')
        raise
