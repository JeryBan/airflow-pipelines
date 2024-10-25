import os

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import os
from core.utils.db import connect_to_postgres, perform_query
from core.utils.data import query_to_csv

CURRENT_DIR = Path(os.getcwd())
DATA_DIR = CURRENT_DIR / 'dags/data'

default_args = {
    'owner': 'JeryBan',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id='eae_dag',
     default_args=default_args,
     start_date=None,
     catchup=False)
def etl():
    @task(retries=2, task_id='get_data')
    def get_total_applications_from_portal(ti):
        conn = connect_to_postgres("eae_db")

        query = '''
        select ea.id from eae_application ea 
        join
            eae_applicant_commitment eac 
            on ea.tax_number = eac.tax_number
        join 
            app_submission_portal asp 
            on eac.submission_portal_id = asp.id 
        where 
            ea.application_status_id = %s and asp.id = %s
        '''
        application_status_id = 2
        portal_id = 569

        queryresult = perform_query(application_status_id, portal_id,
                                    conn=conn,
                                    query=query)

        ti.xcom_push(key='query', value=queryresult)
        # return queryresult

    @task(retries=2, task_id='save_data')
    def save_to_csv(ti):
        data = ti.xcom_pull(task_ids='get_data', key='query')
        query_to_csv(data, DATA_DIR / 'test.csv')

    get_total_applications_from_portal() >> save_to_csv()


etl()
