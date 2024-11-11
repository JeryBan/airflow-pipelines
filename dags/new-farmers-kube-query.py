from datetime import timedelta
import csv

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes import client, config, stream

from core.share import DIRECTORIES

TEMP_DIR = DIRECTORIES.TEMP
CONF_DIR = DIRECTORIES.CONFIG

POD_NAME = Variable.get("new-farmers-staging-worker-pod-name")

config.load_kube_config(config_file=f'{CONF_DIR}/kube/config')

default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


def run_query_in_pod(query):
    v1 = client.CoreV1Api()

    db_hook = PostgresHook(postgres_conn_id='new-farmers-staging-db')
    conn = db_hook.get_connection(conn_id='new-farmers-staging-db')

    exec_command = [
        "sh", "-c",
        f"PGPASSWORD='{conn.password}' psql -h {conn.host} -p {conn.port} -U {conn.login} -d {conn.schema} -c \"{query}\""
    ]

    response = stream.stream(
        v1.connect_get_namespaced_pod_exec,
        POD_NAME,
        'staging',
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
    return f"Command output: {response}"


@dag(
    dag_id='new-farmers-kube-query.py',
    tags=['new-farmers'],
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={'query': None}
)
def kube_query_dag():
    run_query_task_1 = PythonOperator(
        task_id='run_query',
        python_callable=run_query_in_pod,
        op_args=["{{ params.query }}"],
        do_xcom_push=True
    )

    @task
    def query_to_csv_transform(ti):
        response = ti.xcom_pull(task_ids='run_query')
        save_path = f'{TEMP_DIR}/new-farmers-qs.csv'

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

    run_query_task_1 >> query_to_csv_transform()


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
