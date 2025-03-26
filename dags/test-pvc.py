from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

DAG_ID = "python_operator_pvc_example"
SHARED_VOLUME_MOUNT_PATH = "/shared"
FILE_PATH = os.path.join(SHARED_VOLUME_MOUNT_PATH, "my_file.txt")

def create_file():
    with open(FILE_PATH, "w") as f:
        f.write("Hello from task 1!")
    return "File created successfully"

def read_file():
    with open(FILE_PATH, "r") as f:
        content = f.read()
    print(f"File content: {content}")
    return content

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    create_file_task = PythonOperator(
        task_id="create_file",
        python_callable=create_file,
        op_kwargs={},
    )

    read_file_task = PythonOperator(
        task_id="read_file",
        python_callable=read_file,
        op_kwargs={},
    )

    create_file_task >> read_file_task
