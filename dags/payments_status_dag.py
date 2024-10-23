import os
from datetime import timedelta
from pathlib import Path

import requests
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from core.utils.data import export_xls_from_base64

CURRENT_DIR = Path(os.getcwd())
DATA_DIR = CURRENT_DIR / 'dags/data'
CLEANUP = True

default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=20)
}

headers = {
    "Content-Type": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
}


@dag(
    dag_id='payments_status_dag',
    tags=['m2_1'],
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)
def get_payment_status():
    """
    **Required Airflow Variables:**

        * OPSA_USERNAME
        * OPSA_PASSWORD
        * OPSA_LOGIN_URL
        * evaluation_xls_filters
        * commit_xls_filters
    """

    @task(retries=1, task_id='OPSA_login')
    def login(ti):
        """
        Establish a session with OPSA and retrieve cookies.
        """
        session = requests.Session()
        session.headers.update(headers)

        credentials = {
            'login': Variable.get('OPSA_USERNAME'),
            'password': Variable.get('OPSA_PASSWORD')
        }
        login_response = session.post(
            Variable.get('OPSA_LOGIN_URL'),
            json=credentials
        )

        if login_response.status_code == 200:
            ti.xcom_push(key='cookies', value=session.cookies.get_dict())
            return session.cookies.get_dict()
        else:
            raise Exception(f"Login failed with status code {login_response.status_code}: {login_response.text}")

    @task(retries=2)
    def get_evaluation_xls(cookies, **kwargs):
        """
        Fetches the application evaluation xls.
        """
        return base_xls_task(
            ti=kwargs['ti'],
            cookies=cookies,
            request_url="https://osdeopekepe.dikaiomata.gr/RDIIS/rest/PaymentEvaluation/findLazyPaymentEvaluation_toExcel",
            xls_fields=Variable.get('evaluation_xls_filters'),
            filename='evaluation_report.xls'
        )

    @task(retries=2)
    def get_commit_xls(cookies, **kwargs):
        """
        Fetches the payments commit xls.
        """
        return base_xls_task(
            ti=kwargs['ti'],
            cookies=cookies,
            request_url="https://osdeopekepe.dikaiomata.gr/RDIIS/rest/PaymentCollectionEvaluation/findLazyPaymentCollectionEvaluation_toExcel",
            xls_fields=Variable.get('commit_xls_filters'),
            filename='commit_report.xls'
        )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def construct_dataframes(
            evaluation_path=None,
            commit_path=None,
            **kwargs
    ):
        ti = kwargs['ti']

        if not (evaluation_path and commit_path):
            evaluation_path = ti.xcom_pull(task_ids='get_evaluation_xls', key='evaluation_report.xls')
            commit_path = ti.xcom_pull(task_ids='get_commit_xls', key='commit_report.xls')

        evaluation_df = pd.read_excel(evaluation_path)
        commit_df = pd.read_excel(commit_path)

        df = pd.merge(evaluation_df, commit_df, on='Αριθμός Παρτίδας', how='right')
        df_sorted = df.sort_values(by='Αριθμός Παρτίδας', ascending=True)

        save_dir = DATA_DIR / 'tmp'
        Path.mkdir(save_dir, parents=True, exist_ok=True)
        xls_dir = save_dir / 'final_report.xls'

        df_sorted.to_excel(xls_dir, index=False)

        return df_sorted

    @task(depends_on_past=True)
    def cleanup(ti):
        evaluation_path = Path(ti.xcom_pull(task_ids='get_evaluation_xls', key='evaluation_report.xls'))
        commit_path = Path(ti.xcom_pull(task_ids='get_commit_xls', key='commit_report.xls'))

        if evaluation_path.exists():
            os.remove(evaluation_path)
            print(f'{evaluation_path} deleted')

        if commit_path.exists():
            os.remove(commit_path)
            print(f'{commit_path} deleted')

    cookies = login()
    evaluation_xls_path = get_evaluation_xls(cookies=cookies)
    commit_xls_path = get_commit_xls(cookies=cookies)
    ret = construct_dataframes(evaluation_xls_path, commit_xls_path)

    if CLEANUP:
        ret >> cleanup()


run = get_payment_status()


def base_xls_task(
        ti,
        cookies,
        request_url: str,
        xls_fields: dict,
        filename: str,
):
    session = requests.Session()
    session.headers.update(headers)

    if not cookies:
        cookies = ti.xcom_pull(task_ids='get_evaluation_xls', key='cookies')

    session.cookies.update(cookies)

    xls_response = session.post(
        request_url,
        data=xls_fields,
    )

    if xls_response.status_code == 200:
        encoded_data = xls_response.json().get('data')

        if encoded_data:
            xls_path = export_xls_from_base64(
                encoded_data,
                filename=filename
            )
            ti.xcom_push(key=filename, value=xls_path)
            return xls_path
    else:
        raise Exception(
            f"Failed to retrieve xls. Status code: {xls_response.status_code}: {xls_response.content}")


