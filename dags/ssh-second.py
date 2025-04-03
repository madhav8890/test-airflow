from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# List of Hosts - Consider moving these to environment variables or Airflow Variables
host_list = ["10.0.139.192", "10.0.136.28", "10.0.149.221"]

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "timeout": 300,
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
}

# Define DAG
with DAG(
    "ssh_parallel_execution",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['ssh', 'parallel'],
    description='Execute commands in parallel across multiple hosts via SSH',
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Create SSH tasks for each host
    ssh_tasks = []
    for host in host_list:
        task = SSHOperator(
            task_id=f"ssh_task_{host.replace('.', '_')}",
            ssh_conn_id="ssh_remote",
            command="sudo bash {{ params.script_path }}",
            params={"script_path": "/home/ubuntu/get_info.sh"},
            do_xcom_push=True,
            get_pty=True,
            remote_host=host,
            cmd_timeout=300,  # Changed from command_timeout to cmd_timeout
        )
        ssh_tasks.append(task)

    # Set task dependencies
    start >> ssh_tasks >> end

