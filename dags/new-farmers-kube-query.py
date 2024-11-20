import csv
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from core.share import DIRECTORIES, URLS
from core.utils.db import run_query_in_pod

DATA_DIR = DIRECTORIES.DATA
CONF_DIR = DIRECTORIES.CONFIG
DOWNLOAD_URL = URLS.DOWNLOAD


POD_NAME = Variable.get("new-farmers-staging-worker-pod-name")
NAMESPACE = 'staging'
CONN_ID = 'new-farmers-staging-db'
KUBE_CONF_PATH = f'{CONF_DIR}/kube/config'


default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


@dag(
    dag_id='new-farmers-kube-query.py',
    tags=['new-farmers'],
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'query': None,
        'namespace': NAMESPACE,
        'pod_name': POD_NAME,
        'conn_id': CONN_ID,
        'kube_conf_path': KUBE_CONF_PATH
    }
)
def kube_query_dag():
    run_query_task = PythonOperator(
        task_id='run_query',
        python_callable=run_query_in_pod,
        op_kwargs={
            "query": "{{ params.query }}",
            "namespace": "{{ params.namespace }}",
            "pod_name": "{{ params.pod_name }}",
            "conn_id": "{{ params.conn_id }}",
            "kube_config_path": "{{ params.kube_conf_path }}"
        },
        do_xcom_push=True
    )

    @task
    def query_to_csv(ti):
        response = ti.xcom_pull(task_ids='run_query')
        save_path = f'{DATA_DIR}/new-farmers-qs.csv'

        if not response.startswith("Command output: ERROR:"):
            rows = response.splitlines()

            # Extract column headers (skip the 'Command Output:' literal)
            headers = rows[0][15:].split('|')
            headers = [header.strip() for header in headers]

            data = []
            for row in rows[2:-2]:
                row_data = [item.strip() for item in row.split('|')]
                data.append(row_data)

            with open(save_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(data)

            filename = os.path.basename(save_path)
            download_path = f'{DOWNLOAD_URL}/{filename}'

            print(download_path)
            return download_path

    run_query_task >> query_to_csv()


kube_query_dag = kube_query_dag()

# sql_task = KubernetesPodOperator(
#     namespace="staging",  # Namespace where worker pod will be created
#     # image="postgres:latest",  # Image with SQL client tools (use appropriate image for your DB)
#     # cmds=["psql", "-h", "your-database-service-name", "-U", "your_db_user", "-d", "your_db_name", "-c",
#     #       "SELECT * FROM your_table;"],
#     image="alpine:latest",
#     image_pull_secrets=[{"name": "dockerhub-secret"}],
#     cmds=["bash", "-cx", ('echo \'hello from airflow\'')],
#     name="airflow-worker",
#     task_id="airflow-kube",
#     get_logs=True,
#     is_delete_operator_pod=False,
#     in_cluster=False,  # Set to False to use your kubeconfig file
#     config_file=f"{CONF_DIR}/kube/config",
#     do_xcom_push=True
# )
