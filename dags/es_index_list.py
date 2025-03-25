from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from opensearchpy import OpenSearch

# OpenSearch connection details
OS_HOST = "http://elasticsearch-headless.airflow.svc.cluster.local:9200"

def list_opensearch_indices():
    """
    Connect to OpenSearch and list indices.
    """
    try:
        # Connect to OpenSearch
        client = OpenSearch(
            [OS_HOST],
            verify_certs=False
        )

        # Fetch indices
        indices = client.cat.indices(format="json")

        # Log the index details
        for index in indices:
            print(f"Index: {index['index']}, Status: {index['status']}, Docs: {index['docs.count']}, Size: {index['store.size']}")

    except Exception as e:
        print(f"Error fetching indices: {e}")
        raise

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 24),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "elasticsearch_list_indices",
    default_args=default_args,
    description="DAG to list OpenSearch indices",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Define the PythonOperator task
    list_indices_task = PythonOperator(
        task_id="list_opensearch_indices",
        python_callable=list_opensearch_indices
    )

    list_indices_task
