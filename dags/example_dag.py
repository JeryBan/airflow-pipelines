"""
Example dag file to demonstrate
a basic etl pipeline.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from core.utils.db import connect_to_testdb, perform_query, close_connection

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
        conn = connect_to_testdb()

        query = '''
        CREATE TABLE IF NOT EXISTS devs (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50)
        );
        '''

        queryresult = perform_query(conn=conn,
                                     query=query,
                                     save_path=DATA_DIR,
                                     output_type='csv')

        close_connection(conn)
        return queryresult

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
    transform_task = transform(ext)
    load_task = load(transform_task)


example_etl_dag = etl()
