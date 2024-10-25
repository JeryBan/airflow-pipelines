from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='/config/environments/env-ssh')

default_args = {
    'owner': 'cogni',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id="Συμφωνητικα",
     default_args=default_args,
     catchup=False)
def copy_contracts():

    @task.bash(env={
                   'WEB_IP': os.getenv('APP_IP'),
                   'APP_IP': os.getenv('APP_IP'),
                   'WEB_KEY': os.getenv('WEB_KEY'),
                   'APP_KEY': os.getenv('APP_KEY')
                   })
    def web_server_task() -> str:
        return """ssh -i $SSH_KEY root@$WEB_IP << 'ENDSSH'
                    scp -i $APP_KEY -r root@$APP_IP:/path/to/data /tmp/data
                ENDSSH
                """

    @task.bash(cwd="/workspace",
               trigger_rule=TriggerRule.ALL_SUCCESS,
               env={'IP': os.getenv('WEB_IP')})
    def copy_from_web() -> str:
        return "scp -r root@$IP:/tmp/data ."

    web_server_task() >> copy_from_web()


run = copy_contracts()
