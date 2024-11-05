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
    def copy_file_from_db(remote_filepath: str):
        copy_from_db = SSHOperator(
            task_id='retrieve_dump_from_db',
            ssh_conn_id='m21_webserver',
            conn_timeout=None,
            cmd_timeout=None,
            command=f'scp database_server:{remote_filepath} /tmp/',
        )
        copy_from_db.execute(context={})

    @task
    def retrieve_file_from_webserver(remote_filepath: str):
        name_idx = remote_filepath.index("ypaat-backup-")
        filename = remote_filepath[name_idx:]

        sftp_remote_filepath = f'/tmp/{filename}'
        local_filepath = DUMP_DIR / filename

        sftp_hook = SFTPHook(ssh_conn_id="m21_webserver")
        sftp_hook.retrieve_file(sftp_remote_filepath, local_filepath)

        return filename

    @task
    def remove_blobs(filename: str):
        local_filepath = DUMP_DIR / filename
        remove_blob_columns(local_filepath, local_filepath)

    @task
    def cleanup(filename: str):
        c = SSHOperator(
            task_id='clean_up',
            ssh_conn_id='m21_webserver',
            conn_timeout=None,
            cmd_timeout=None,
            command=f'rm -f /tmp/{filename}'
        )
        c.execute(context={})

    remote_filepath = get_filepath()
    filename = retrieve_file_from_webserver(remote_filepath)

    copy_file_from_db(remote_filepath) >> filename >> [remove_blobs(filename), cleanup(filename)]


m21_latest_dump_dag = fetch_m21_dump()
