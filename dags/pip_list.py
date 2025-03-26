from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def list_installed_packages():
    """Function to list all installed Python packages."""
    result = subprocess.run(["pip", "list"], capture_output=True, text=True)
    print("Installed Python Packages:\n", result.stdout)

# Define the DAG
with DAG(
    dag_id="list_python_packages",
    schedule_interval=None,  # Run manually
    start_date=datetime(2025, 3, 26),
    catchup=False,
) as dag:

    list_packages_task = PythonOperator(
        task_id="list_packages",
        python_callable=list_installed_packages
    )

    list_packages_task  # Task execution


