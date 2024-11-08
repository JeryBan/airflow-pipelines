from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from core.share import DIRECTORIES

DUMP_DIR = DIRECTORIES.DUMPS

default_args = {
    'owner': 'Cognitera',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


@dag(
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
    def connect(ti):
        hook = PostgresHook(postgres_conn_id='airflow_db')
        conn = hook.get_conn()

        db_user = conn.info.user
        db_password = conn.info.password
        db_host = conn.info.host
        db_port = conn.info.port
        db_name = conn.info.dbname

        timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
        filename = f'airflow_save_{timestamp}.dump'
        of = DUMP_DIR / filename

        Variable.set('PGPASSWORD', db_password)

        cmd = f"pg_dump -U {db_user} -h {db_host} -p {db_port} -d {db_name} -F c -b -v -f {of}"
        return cmd

    @task.bash(env={'PGPASSWORD': Variable.get('PGPASSWORD', "")})
    def create_dump(command: str):
        return command

    command = connect()
    create_dump(command)


save_game = save_game()
