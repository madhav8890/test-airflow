from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def task1(**kwargs):
    data = {"message": "Hello from task1!"}
    kwargs['ti'].xcom_push(key='data_key', value=json.dumps(data))  # Push to XCom

def task2(**kwargs):
    ti = kwargs['ti']
    received_data = ti.xcom_pull(task_ids='task1', key='data_key')  # Pull from XCom
    data = json.loads(received_data)
    print(f"Received in Task2: {data['message']}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 30),
    'retries': 1,
}

with DAG('xcom_example_dag', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['test', 'xcom'],) as dag:

    t1 = PythonOperator(
        task_id='task1',
        python_callable=task1,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='task2',
        python_callable=task2,
        provide_context=True
    )

    t1 >> t2  # Define task order
