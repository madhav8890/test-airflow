from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import logging

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'elasticsearch_list_indices',
    default_args=default_args,
    description='DAG to list all Elasticsearch indices',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2024, 3, 25),
    tags=['elasticsearch']
)

ES_HOST = "http://elasticsearch-headless.airflow.svc.cluster.local:9200"

def list_indices():
    """Fetch and log all indices in Elasticsearch."""
    try:
        es = Elasticsearch([ES_HOST])

        indices = es.cat.indices(format='json')  # Fetch indices as JSON
        log.info(f"Total Indices: {len(indices)}")

        for index in indices:
            log.info(f"Index: {index['index']}, Status: {index['status']}, Docs: {index['docs.count']}, Size: {index['store.size']}")

    except Exception as e:
        log.error(f"Error fetching indices: {str(e)}")

list_indices_task = PythonOperator(
    task_id='list_elasticsearch_indices',
    python_callable=list_indices,
    dag=dag
)

list_indices_task

