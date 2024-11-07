from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from core.share import DIRECTORIES

TEMP_DIR = DIRECTORIES.TEMP
CONF_DIR = DIRECTORIES.CONFIG

default_args = {
    'owner': 'Cognitera',
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
def kube_test_dag():

    sql_task = KubernetesPodOperator(
        namespace="staging",  # Namespace where worker pod will be created
        # image="postgres:latest",  # Image with SQL client tools (use appropriate image for your DB)
        # cmds=["psql", "-h", "your-database-service-name", "-U", "your_db_user", "-d", "your_db_name", "-c",
        #       "SELECT * FROM your_table;"],
        image="alpine:latest",
        image_pull_secrets=[{"name": "dockerhub-secret"}],
        cmds=["bash", "-cx", ('echo \'hello from airflow\'')],
        name="airflow-worker",
        task_id="airflow-kube",
        get_logs=True,
        is_delete_operator_pod=False,
        in_cluster=False,  # Set to False to use your kubeconfig file
        config_file=f"{CONF_DIR}/kube/config",
        do_xcom_push=True
    )


kube_test_dag = kube_test_dag()
