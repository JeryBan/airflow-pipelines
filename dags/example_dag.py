"""
Example dag file to demonstrate
a basic etl pipeline.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DATA_DIR = Path('./data')

default_args = {
    'owner': 'JeryBan',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id='example_taskflow',
     default_args=default_args,
     start_date=datetime(2024, 1, 26),  # year-month-day
     schedule_interval='@daily',  # crontab syntax
     catchup=False)
def etl():
    @task(retries=2)
    def extract():
        SQLExecuteQueryOperator(
            task_id='fetch_query',
            conn_id='test_localhost',
            sql="INSERT INTO users (name) VALUES ('airflow_dag')",
            autocommit=True
        )

        return 'user created'

    @task(multiple_outputs=True)
    def transform(extracted_data):
        print(extracted_data)
        data = {
            'values': [True, True, False],
            'labels': [1, 2, 3]
        }
        return data

    @task()
    def load(transformed_data):
        print(transformed_data)
        return 'loading data'

    # schema
    ext = extract()
    transform = transform(ext)
    load = load(transform)


example_etl_dag = etl()
