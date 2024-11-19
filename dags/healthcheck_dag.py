from datetime import timedelta, datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

default_args = {
    'owner': 'Cognitera'
}

apps = [
    {"name": "m21", "url": "https://advisoryservices.agrotikianaptixi.gr/sign-in?0"},
    {"name": "new_farmers", "url": "http://ui.new-farmers.lab.cognitera.gr/health"},
]

email_recipients = [
    "nstathopoulos@cognitera.gr"
]


# TODO: Configure smtp server for emails
def email_on_failure(context):
    ti = context.get('task_instance')
    task_id = ti.task_id
    log_url = ti.log_url
    subject = f"Healthcheck Failed: {task_id}"
    body = f"""
        Task {task_id} has failed.

        Logs: {log_url}
        """
    send_email(to=email_recipients, subject=subject, html_content=body)


@dag(
    tags=['cognitera'],
    default_args=default_args,
    start_date=datetime(2024, 11, 19),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
)
def healthcheck_dag():
    for app in apps:
        BashOperator(
            task_id=f'{app["name"]}-healthcheck',
            bash_command=f'curl -f -m 10 {app["url"]}',
            on_failure_callback=email_on_failure,
            retries=2,
            retry_delay=timedelta(seconds=30)
        )


healthcheck_dag = healthcheck_dag()
