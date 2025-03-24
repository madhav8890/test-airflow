from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2024, 3, 23), schedule_interval="@daily", catchup=False)
def my_taskflow_dag():
    
    @task
    def extract():
        return "Extracted Data"

    @task
    def transform(data):
        return f"Transformed {data}"

    @task
    def load(data):
        print(f"Loading: {data}")

    # Task Dependencies
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

my_taskflow_dag()

