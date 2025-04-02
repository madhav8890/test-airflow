from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import datetime, timedelta

# List of Hosts
host_list = ["10.0.139.192", "10.0.136.28", "10.0.149.221"]  # Add more hosts as needed

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "timeout": 300,  # 5 minutes timeout
    "depends_on_past": False,
}

# Define DAG
dag = DAG(
    "ssh_parallel_test_dag",
    default_args=default_args,
    schedule_interval=None,  # Run manually
    start_date=datetime(2024, 1, 1),  # Use explicit datetime instead of days_ago
    catchup=False,
    tags=['ssh', 'parallel'],
    description='Execute commands in parallel across multiple hosts via SSH',
)

# Function to create SSH tasks dynamically
@task
def generate_ssh_tasks(host):
    return SSHOperator(
        task_id=f"ssh_task_{host.replace('.', '_')}",
        ssh_conn_id="ssh_remote",  # Airflow Connection ID (Make sure it works for all hosts)
        command="sudo bash {{ params.script_path }}",
        params={"script_path": "/home/ubuntu/get_info.sh"},
        dag=dag,
        do_xcom_push=False,  # Disable XCom for efficiency
        get_pty=True,  # Allocate pseudo-terminal to handle sudo commands better
    )

# Expand tasks dynamically for each host
ssh_tasks = generate_ssh_tasks.expand(host=host_list)

