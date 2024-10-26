import os
from datetime import timedelta, datetime
from pathlib import Path

import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


CURRENT_DIR = Path(os.getcwd())
DUMP_DIR = CURRENT_DIR / "dags/dumps"

default_args = {
    'owner': 'Cognitera',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

@dag(
    id='save_game_dag',
    tags=['airflow'],
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
)
def save_game():
    """
    Airflow database backup dag.

    Requires:
        conn_id: airflow_db
    """

    @task
    def connect():
        hook = PostgresHook(postgres_conn_id='airflow_db')
        conn = hook.get_conn()

        db_user = conn.login
        db_password = conn.password
        db_host = conn.host
        db_port = conn.port
        db_name = conn.dbname

        Path.mkdir(DUMP_DIR, parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
        filename = f'airflow_save_{timestamp}.dump'
        of = DUMP_DIR / filename

        os.environ['PGPASSWORD'] = db_password

        cmd = f"pg_dump -U {db_user} -h {db_host} -p {db_port} -d {db_name} -F c -b -v -f {of}"
        return cmd

    @task.bash(env={'PGPASSWORD': os.getenv('PGPASSWORD')})
    def create_dump(command):
        return command

    command = connect()
    create_dump(command)

save_game()
