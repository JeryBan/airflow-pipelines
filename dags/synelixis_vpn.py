from datetime import timedelta

from airflow.decorators import dag, task
from core.share import DIRECTORIES

VPN_DIR = f'{DIRECTORIES.CONFIG}/vpns'

default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


@dag(
    dag_id='open_synelixis_vpn',
    tags=['util'],
    default_args=default_args,
    schedule=None
)
def open_synelixis_vpn():

    @task.bash
    def start_vpn():
        vpn_file = f'{VPN_DIR}/vpn.ovpn'

        return ""

    start_vpn = start_vpn()


open_synelixis_vpn = open_synelixis_vpn()