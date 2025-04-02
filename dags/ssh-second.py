from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task
from datetime import datetime, timedelta

# List of Hosts
host_list = ["10.0.139.192", "10.0.136.28", "10.0.149.221"]

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "timeout": 300,
    "depends_on_past": False,
}

# Define DAG
with DAG(
    "ssh_parallel_test_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ssh', 'parallel'],
    description='Execute commands in parallel across multiple hosts via SSH',
) as dag:

    # Start task
    start = DummyOperator(task_id='start')
    
    # End task
    end = DummyOperator(task_id='end')

    # Create SSH tasks for each host
    ssh_tasks = []
    for host in host_list:
        task = SSHOperator(
            task_id=f"ssh_task_{host.replace('.', '_')}",
            ssh_conn_id="ssh_remote",
            command="sudo bash {{ params.script_path }}",
            params={"script_path": "/home/ubuntu/get_info.sh"},
            do_xcom_push=False,
            get_pty=True,
        )
        ssh_tasks.append(task)

    # Set task dependencies
    start >> ssh_tasks >> end

