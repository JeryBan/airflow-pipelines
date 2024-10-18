import os
from datetime import timedelta
from pathlib import Path

import requests
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable

from core.utils.data import export_xls_from_base64

CURRENT_DIR = Path(os.getcwd())
DATA_DIR = CURRENT_DIR / 'dags/data'

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
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)
def get_payment_status():
    """
    Airflow Variables needed:
        - OPSA_USERNAME
        - OPSA_PASSWORD
        - OPSA_LOGIN_URL
        - evaluation_xls_filters
        - commit_xls_filters
        - bank_xls_filters
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
    def get_evaluation_xls(cookies, ti):
        """
        Fetches the application evaluation xls.
        """
        return base_xls_task(
            ti=ti,
            cookies=cookies,
            request_url="https://osdeopekepe.dikaiomata.gr/RDIIS/rest/PaymentEvaluation/findLazyPaymentEvaluation_toExcel",
            xls_fields=Variable.get('evaluation_xls_filters'),
            filename='evaluation_report.xls'
        )

    @task(retries=2)
    def get_commit_xls(cookies, ti):
        """
        Fetches the payments commit xls.
        """
        return base_xls_task(
            ti=ti,
            cookies=cookies,
            request_url="https://osdeopekepe.dikaiomata.gr/RDIIS/rest/PaymentCollectionEvaluation/findLazyPaymentCollectionEvaluation_toExcel",
            xls_fields=Variable.get('commit_xls_filters'),
            filename='commit_report.xls'
        )

    @task(retries=2)
    def get_bank_xls(cookies, ti):
        """
        Fetches the payments commit xls.
        """
        return base_xls_task(
            ti=ti,
            cookies=cookies,
            request_url="https://osdeopekepe.dikaiomata.gr/RDIIS/rest/BankPayCollection/findLazyBankPayCollection_toExcel",
            xls_fields=Variable.get('bank_xls_filters'),
            filename='bank_report.xls'
        )

    @task.branch()
    def branch_node(ti):
        evaluation_xls_path = ti.xcom_pull(task_ids='get_evaluation_xls', key='path')
        commit_xls_path = ti.xcom_pull(task_ids='get_commit_xls', key='path')
        base_xls_path = ti.xcom_pull(task_ids='get_bank_xls', key='path')

        if Path(evaluation_xls_path).exists() \
                and Path(commit_xls_path).exists() \
                and Path(base_xls_path).exists():
            return 'construct_dataframes'

    @task
    def construct_dataframes(
            ti,
            evaluation_xls_path: str = None,
            commit_xls_path: str = None,
            bank_xls_path: str = None,
    ):
        if not (evaluation_xls_path and commit_xls_path and bank_xls_path):
            evaluation_xls_path = ti.xcom_pull(task_ids='get_evaluation_xls', key='path')
            commit_xls_path = ti.xcom_pull(task_ids='get_commit_xls', key='path')
            bank_xls_path = ti.xcom_pull(task_ids='get_bank_xls', key='path')

        evaluation_df = pd.read_excel(evaluation_xls_path)
        commit_df = pd.read_excel(commit_xls_path)
        bank_df = pd.read_excel(bank_xls_path)

        df_tmp = pd.merge(evaluation_df, commit_df, on='Αριθμός Παρτίδας')
        df = pd.merge(df_tmp, bank_df, on='Αριθμός Παρτίδας')

        return df

    cookies = login()
    evaluation_xls_path = get_evaluation_xls(cookies=cookies)
    commit_xls_path = get_commit_xls(cookies=cookies)
    bank_xls_path = get_bank_xls(cookies=cookies)
    construct_dataframes(evaluation_xls_path, commit_xls_path, bank_xls_path)


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
            ti.xcom_push(key='path', value=xls_path)
            return xls_path
    else:
        raise Exception(
            f"Failed to retrieve xls with status code {xls_response.status_code}: {xls_response.content}")
