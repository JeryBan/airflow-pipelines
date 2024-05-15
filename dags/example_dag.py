"""
Example dag file to demonstrate
a basic etl pipeline.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta

from pathlib import Path
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
        # connect to a db
        conn = connect_to_testdb()

        # query data from db
        SQL = "SELECT * FROM users"
        save_path = DATA_DIR / 'example.csv'
        queryresult = perform_query(
            connection=conn,
            query=SQL,
            output_type='csv',
            save_path=save_path
        )

        # close connection
        close_connection(conn)
        return save_path

    @task(multiple_outputs=True)
    def transform(extracted_data):
        return 'transforming data'

    @task()
    def load(transformed_data):
        return 'loading data'

    # schema
    ext = extract()
    transform = transform(ext)
    load = load(transform)


example_etl_dag = etl()
