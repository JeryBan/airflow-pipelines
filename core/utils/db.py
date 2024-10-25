"""
Utility functions to manipulate connections
to databases.
"""
import logging

import requests
from airflow.models import connection, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.connection import Connection
from eae_api_client.eaeapiclient import EaeApiClient


class EaeConnectionManager:
    """
    Connection manager for Eae databases.
    """

    def __init__(self):
        self.client = None
        self.conn = None

    def get_db_connection(self):
        """
        Returns a connection with eae database.
        """
        hook = PostgresHook(postgres_conn_id="eae_vpn")
        try:
            self.conn = hook.get_conn()
            return self.conn
        except ConnectionRefusedError as e:
            logging.error(f'Connection Refused: {e}')
            raise requests.exceptions.ConnectionError

    def get_api_client(self) -> EaeApiClient:
        """
        Returns a connection with Eae database.

        Airflow Variables needed:

            * EAE_BACKEND_URL
            * EAE_TOKEN
        """
        eae_backend_url = Variable.get('EAE_BACKEND_URL').rstrip("/")
        token = Variable.get('EAE_TOKEN')

        authenticated_token = EaeConnectionManager._validate_authentication_token(token)

        self.client = EaeApiClient(eae_backend_url, authenticated_token)
        return self.client

    def perform_db_query(self, query, many=True, *args, **kwargs):
        if self.conn is None:
            self.conn = self.get_db_connection()

        return perform_query(query, self.conn, many, *args)

    @staticmethod
    def _validate_authentication_token(authentication_token):
        if len(authentication_token) == 1:
            msg = 'Invalid token header. No credentials provided.'
            logging.error(msg)
            raise ValueError(msg)
        elif len(authentication_token) > 2:
            msg = 'Invalid token header. Token string should not contain spaces.'
            logging.error(msg)
            raise ValueError(msg)

        try:
            return authentication_token[1].decode()
        except (UnicodeError, IndexError):
            msg = 'Invalid token header. Token string should not contain invalid characters.'
            logging.error(msg)
            raise ValueError(msg)


def perform_query(query: str,
                  conn: connection,
                  many=True,
                  *args):
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
                query_result = cursor.fetchall()
            else:
                query_result = cursor.fetchone()
            return query_result
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise
    finally:
        cursor.close()


def connect_to_postgres(database: str) -> connection:
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


def close_connection(conn: Connection) -> None:
    """Close a connection"""
    try:
        conn.close()
    except ConnectionError as e:
        logging.error(f'Error closing a connection: {e}')
        raise
