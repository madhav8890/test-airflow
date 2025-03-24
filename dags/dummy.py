from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dummy_data_pipeline',
    default_args=default_args,
    description='A simple DAG for ETL process',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Dummy function tasks
def extract_data():
    print("Extracting data...")

def transform_data():
    print("Transforming data...")

def load_data():
    print("Loading data...")

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Define Task Dependencies
start >> extract_task >> transform_task >> load_task >> end

