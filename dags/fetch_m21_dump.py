from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.operators.ssh import SSHOperator

from core.share import DIRECTORIES
from core.utils.db import remove_blob_columns

DUMP_DIR = DIRECTORIES.DUMPS
TEMP_DIR = DIRECTORIES.TEMP

default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}

available_dumps_txt = TEMP_DIR / 'available_m21_dumps.txt'


def read_keys_from_file(file_path) -> dict:
    """
    Read the contents of available_m21_dumps.txt and return a dictionary
    where keys are the lines from the file.
    """
    if not Path(file_path).is_file():
        raise FileNotFoundError(f"{file_path} does not exist.")

    with open(file_path, 'r') as file:
        # Read lines and strip whitespace
        keys = [line.strip() for line in file if line.strip()]

    d = {key: False for key in keys}
    return d


@dag(
    tags=['m21'],
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params=read_keys_from_file(available_dumps_txt)
)
def fetch_m21_dump():
    """
    **Requires:**

        * conn_id: m21_webserver
        * fetch_available_m21_dumps_list dag needs to be run first
    """

    @task
    def get_filepath(**kwargs):
        for k, v in kwargs['params'].items():
            if v:
                return k

    @task
    def copy_file_from_db(**kwargs):
        remote_filepath = kwargs['ti'].xcom_pull(task_ids="get_filepath")

        copy_from_db = SSHOperator(
            task_id='retrieve_dump_from_db',
            ssh_conn_id='m21_webserver',
            conn_timeout=None,
            cmd_timeout=None,
            command=f'scp database_server:{{ remote_filepath }} /tmp/m21_dump.sql',
        )
        copy_from_db.execute(context={})

    @task
    def retrieve_file_from_webserver(**kwargs):
        remote_filepath = '/tmp/m21_dump.sql'
        local_filepath = DUMP_DIR / 'm21_dump.sql'

        sftp_hook = SFTPHook(ssh_conn_id="m21_webserver")
        sftp_hook.retrieve_file(remote_filepath, local_filepath)

    @task
    def remove_blobs(**kwargs):
        local_filepath = DUMP_DIR / 'm21_dump.sql'
        remove_blob_columns(local_filepath, local_filepath)

    cleanup = SSHOperator(
        task_id='clean_up',
        ssh_conn_id='m21_webserver',
        conn_timeout=None,
        cmd_timeout=None,
        command='rm -f /tmp/m21_dump.sql',
    )

    get_filepath() >> copy_file_from_db() >> retrieve_file_from_webserver() >> [remove_blobs(), cleanup]


m21_latest_dump_dag = fetch_m21_dump()
