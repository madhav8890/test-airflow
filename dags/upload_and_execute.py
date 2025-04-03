from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# List of target servers
host_list = ["10.0.152.181", "10.0.136.28", "10.0.149.221"]

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="upload_and_execute_github_script",
    default_args=default_args,
    description="Upload script from GitHub and execute it on remote servers",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Dummy start task
    start = DummyOperator(task_id="start")

    # Task 1: Upload script to each server
    upload_tasks = []
    for host in host_list:
        upload_task = SSHOperator(
            task_id=f"upload_script_{host.replace('.', '_')}",
            ssh_conn_id="ssh_remote",  # Connection ID configured in Airflow
            command="""
            wget -O /tmp/dummy.sh https://raw.githubusercontent.com/madhav8890/test-airflow/585049c57911e81116a4a15aa612179e8296127a/script/dummy.sh &&
            chmod +x /tmp/dummy.sh
            """,
            remote_host=host,
            cmd_timeout=300,  # Changed from command_timeout to cmd_timeout
        )
        upload_tasks.append(upload_task)

    # Task 2: Execute script on each server
    execute_tasks = []
    for host in host_list:
        execute_task = SSHOperator(
            task_id=f"execute_script_{host.replace('.', '_')}",
            ssh_conn_id="ssh_remote",  # Connection ID configured in Airflow
            command="/tmp/dummy.sh",
            remote_host=host,
            cmd_timeout=300,  # Changed from command_timeout to cmd_timeout
        )
        execute_tasks.append(execute_task)

    # Dummy end task
    end = DummyOperator(task_id="end")

    # Set task dependencies for upload tasks
    start >> upload_tasks

    # Set dependencies between upload tasks and execute tasks
    for upload_task, execute_task in zip(upload_tasks, execute_tasks):
        upload_task >> execute_task

    # Set dependencies for execute tasks to end task
    execute_tasks >> end
    # # Set task dependencies
    # start >> upload_tasks >> execute_tasks >> end
