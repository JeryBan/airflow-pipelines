from datetime import timedelta

from airflow.decorators import dag, task

from core.utils.db import EaeConnectionManager

default_args = {
    'owner': 'Cognitera',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


@dag(
    tags=['example'],
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'token': None,
        'tax_number': None
    }
)
def test_eae_dag():
    @task
    def get_client(**kwargs):
        tax_number = kwargs['params'].get('tax_number', None)

        eae_manager = EaeConnectionManager()

        client = eae_manager.get_api_client(**kwargs)
        id = client.get_item(
                app='eae', endpoint=f'applications/{tax_number}/final/'
            ).get('id')

        res = client.get_item(
            app="eae",
            endpoint=f"/applications/{id}",
        )
        return res

    get_client = get_client()


test_eae_dag = test_eae_dag()
