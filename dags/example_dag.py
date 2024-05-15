from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'JeryBan',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id='example_taskflow',
     default_args=default_args)
def etl():

    @task()
    def extract():
        return 0

    @task()
    def transform(extracted_data):
        return 0

    @task()
    def load(transformed_data):
        return 0

    ext = extract()
    transform = transform(ext)
    load = load(transform)


example_etl_dag = etl()
