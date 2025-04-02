from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

# Define DAG
dag = DAG(
    "ssh_test_dag",
    default_args={"owner": "airflow"},
    schedule_interval=None,  # Run manually
    start_date=days_ago(1),
    catchup=False,
)

# SSH Task: Runs "date" on the remote server
ssh_task = SSHOperator(
    task_id="test_ssh_connection",
    ssh_conn_id="ssh_remote",  # Airflow Connection ID
    command="sudo bash {{ params.script_path }}",
    params={"script_path": "/home/ubuntu/get_info.sh"},
    dag=dag,
)

ssh_task  # Task execution
