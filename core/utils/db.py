"""
Utility functions to manipulate connections
to databases.
"""
import logging
import re

import requests
from airflow.models import connection, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from eae_api_client.eaeapiclient import EaeApiClient

from core.share import DIRECTORIES

CONF_DIR = DIRECTORIES.CONFIG

class EaeConnectionManager:
    """
    Connection manager for Eae databases.
    """

    def __init__(self):
        self.client = None
        self.token = None
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

    def get_api_client(self, **kwargs) -> EaeApiClient:
        """
        Returns a connection with Eae database.

        Airflow Variables needed:

            * EAE_BACKEND_URL
            * token (in kwargs['params']): The authentication token for EAE.
        """
        eae_backend_url = Variable.get('EAE_BACKEND_URL').rstrip("/")
        token = EaeConnectionManager.extract_token(**kwargs)

        self.client = EaeApiClient(eae_backend_url, token)
        return self.client

    def select_query(self, query, many=True, *args, **kwargs):
        if self.conn is None:
            self.conn = self.get_db_connection()

        return perform_select(query, self.conn, many, *args)

    @staticmethod
    def extract_token(**kwargs) -> str:
        try:
            token = kwargs['params']['token']
            return EaeConnectionManager._validate_authentication_token(token)
        except KeyError:
            msg = 'Token missing. Ensure you are correctly passing the token through @dag params keyword argument.'
            logging.error(msg)
            raise KeyError(msg)

    @staticmethod
    def _validate_authentication_token(authentication_token):
        if not authentication_token:
            msg = 'Authentication token is null'
            logging.error(msg)
            raise ValueError(msg)

        if ' ' in authentication_token:
            msg = 'Invalid token. Token string should not contain spaces.'
            logging.error(msg)
            raise ValueError(msg)

        return authentication_token


def perform_select(query: str,
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


def remove_blob_columns(input_file, output_file):
    try:
        with open(input_file, 'rb') as file:
            lines = file.readlines()
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        return

    with open(output_file, 'wb') as file:
        for line in lines:
            try:
                line_decoded = line.decode('utf-8')
                if not re.search(r'\bBLOB\b', line_decoded, re.IGNORECASE):
                    file.write(line)
            except UnicodeDecodeError:
                continue


def run_query_in_pod(
        query: str,
        conn_id: str,
        pod_name: str,
        namespace: str,
        kube_config_path: str,
):
    """
    Executes a PostgreSQL query inside a specified Kubernetes pod.

    This function connects to a Kubernetes pod and runs a PostgreSQL query using
    credentials obtained from the specified Airflow connection.

    Args:
        query (str): The SQL query to execute in the pod.
        conn_id (str): The Airflow connection ID for the PostgreSQL database.
        pod_name (str): The name of the Kubernetes pod where the query should be executed.
        namespace (str): The Kubernetes namespace in which the pod is located.
        kube_config_path (str): Path to the Kubernetes configuration file used to authenticate and connect.

    Returns:
        str: Output from the command executed inside the pod, including query results or any error messages.
    """
    from kubernetes import client, config, stream

    config.load_kube_config(config_file=kube_config_path)
    v1 = client.CoreV1Api()

    db_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = db_hook.get_connection(conn_id=conn_id)

    exec_command = [
        "sh", "-c",
        f"PGPASSWORD='{conn.password}' psql -h {conn.host} -p {conn.port} -U {conn.login} -d {conn.schema} -c \"{query}\""
    ]

    response = stream.stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
    return f"Command output: {response}"

# for kati in katiallo():
#     if kati.startwith("staging-worker"):
#         pod_neme = kati