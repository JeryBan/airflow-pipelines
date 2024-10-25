"""
Utility functions to manipulate connections
to databases.
"""
import logging
import requests

from airflow.models import connection, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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


class S3BucketManager:
    """
    Manager to handle S3 bucket connections and file operations.
    """
    def __init__(self, aws_conn_id='minio_localhost', bucket_name=None):
        """
        Args:
            aws_conn_id (str, optional): airflow connection id. Defaults to 'minio_localhost'.
            bucket_name (str, optional): S3 bucket name. Set if you want to operate on a single bucket.
        """
        self.hook = S3Hook(aws_conn_id=aws_conn_id)
        self.bucket_name = bucket_name

    def upload_file(
            self,
            local_file_path: str,
            destination_path: str,
            bucket_name: str = None,
            replace=True
    ) -> str:
        """
        Uploads a file from a local path to a specified destination path within an S3 bucket.

        Args:
            local_file_path (str): Local path of the file to upload.
            destination_path (str): Destination path within the S3 bucket.
            bucket_name (str, optional): Name of the S3 bucket. Uses the class default if not provided.
            replace (bool, optional): If True, replaces any existing file at the destination path. Default is True.

        Returns:
            str: The S3 URI of the uploaded file.
        """
        if bucket_name:
            self.bucket_name = bucket_name

        self.hook.load_file(
            filename=local_file_path,
            key=destination_path,
            bucket_name=self.bucket_name,
            replace=replace
        )

        return f's3://{bucket_name}/{destination_path}'

    def download_file(
            self,
            destination_path: str,
            local_path: str,
            bucket_name: str = None,
            **kwargs
    ) -> str:
        """
        Downloads a file from a specified path within an S3 bucket to a local path.

        Args:
            destination_path (str): Path of the file within the S3 bucket.
            local_path (str): Local path to save the downloaded file.
            bucket_name (str, optional): Name of the S3 bucket. Uses the class default if not provided.
            **kwargs: Optional parameters.
                use_autogenerated_subdir (bool, optional): If True, adds an autogenerated subdirectory to the download location.

        Returns:
            str: The path to the downloaded file.
        """
        if bucket_name:
            self.bucket_name = bucket_name

        auto = kwargs.get('use_autogenerated_subdir', False)

        f = self.hook.download_fileobj(
            key=destination_path,
            local_path=local_path,
            bucket_name=self.bucket_name,
            preserve_file_name=True,
            use_autogenerated_subdir=auto
        )
        return f

    def get_by_pattern(
            self,
            pattern: str,
            bucket_name: str = None,
            **kwargs
    ):
        """
        Fetches files from an S3 bucket based on a specified pattern.

        Args:
            pattern (str): Pattern for matching file names within the S3 bucket.
            bucket_name (str, optional): Name of the S3 bucket. Uses the class default if not provided.
            **kwargs: Optional parameters.
                delimiter (str, optional): The delimiter to use for folder separation.

        Returns:
            str: The S3 key of the matching file(s).
        """
        if bucket_name:
            self.bucket_name = bucket_name

        delimiter = kwargs.get('delimiter', '')

        f = self.hook.get_wildcard_key(
            wildcard_key=pattern,
            bucket_name=self.bucket_name,
            delimiter=delimiter,
            wildcard_match=True
        )
        return f
