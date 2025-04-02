from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task

# List of Hosts
host_list = ["10.0.139.192", "10.0.136.28", "10.0.149.221"]  # Add more hosts as needed

# Define DAG
dag = DAG(
    "ssh_parallel_test_dag",
    default_args={"owner": "airflow"},
    schedule_interval=None,  # Run manually
    start_date=days_ago(1),
    catchup=False,
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
    )

# Expand tasks dynamically for each host
ssh_tasks = generate_ssh_tasks.expand(host=host_list)

