import tempfile
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.ssh.hooks.ssh import SSHHook
from paramiko import SSHClient, AutoAddPolicy

from core.share import DIRECTORIES

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
    def copy_file_through_webserver(**kwargs):
        remote_filepath = kwargs['ti'].xcom_pull(task_ids="get_filepath")
        local_filepath = DUMP_DIR / 'm21_dump.sql'

        # Get webserver connection
        web_hook = SSHHook(ssh_conn_id="m21_webserver")

        # Connect to webserver using system's default SSH config
        web_client = SSHClient()
        web_client.set_missing_host_key_policy(AutoAddPolicy())
        web_client.connect(
            hostname=web_hook.remote_host,
            username=web_hook.username,
            look_for_keys=True,
            allow_agent=True
        )

        # Create a temporary directory for intermediate transfer
        with tempfile.TemporaryDirectory() as temp_dir:
            # rsync from database server to webserver's temp directory
            stdin, stdout, stderr = web_client.exec_command(
                f'rsync -av database_server:{remote_filepath} {temp_dir}/'
            )
            if stdout.channel.recv_exit_status() != 0:
                raise Exception(f"Rsync from database server failed: {stderr.read().decode()}")

            # SFTP to get the file from webserver to local
            sftp = web_client.open_sftp()
            temp_filepath = next(Path(temp_dir).iterdir())
            sftp.get(str(temp_filepath), str(local_filepath))

            sftp.close()

        web_client.close()

    get_filepath() >> copy_file_through_webserver()


m21_latest_dump_dag = fetch_m21_dump()
