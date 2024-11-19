from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.trigger_rule import TriggerRule

from core.share import DIRECTORIES
from core.utils.vpn import VPNHook

TEMP_DIR = DIRECTORIES.TEMP

default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


@dag(
    tags=['m21'],
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
def fetch_available_m21_dumps_list():
    """
    **Requires:**

        * conn_id: m21_webserver
        * conn_id: synelixis_vpn
    """

    @task
    def open_vpn():
        vpn_hook = VPNHook(conn_id='synelixis_vpn')
        vpn_hook.start_vpn()
        return 'Vpn opened'

    command = """
    ssh database_server 'ls /database/exports/ypaat-backup-* >> dumps.txt && exit'
    scp database_server:/root/dumps.txt /tmp
    ssh database_server 'rm /root/dumps.txt && exit'
    """

    ssh = SSHOperator(
        task_id='fetch_m21_dumps',
        ssh_conn_id='m21_webserver',
        conn_timeout=None,
        cmd_timeout=None,
        command=command,
    )

    @task
    def get_filenames():
        sftp_hook = SFTPHook(ssh_conn_id="m21_webserver")

        remote_filepath = '/tmp/dumps.txt'
        local_filepath = TEMP_DIR / 'available_m21_dumps.txt'

        sftp_hook.retrieve_file(remote_filepath, local_filepath)

    cleanup = SSHOperator(
        task_id='cleanup_m21_dumps',
        ssh_conn_id='m21_webserver',
        command='rm /tmp/dumps.txt',
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def close_vpn():
        vpn_hook = VPNHook(conn_id='synelixis_vpn')
        vpn_hook.stop_vpn()
        return 'Vpn closed'

    open_vpn() >> ssh >> get_filenames() >> \
    cleanup >> close_vpn()


fetch_available_m21_dumps = fetch_available_m21_dumps_list()
