from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import logging
import re

log = logging.getLogger(__name__)

PARENT_DAG_NAME = 'ELASTICSEARCH_INDEX_ROTATION'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    description='DAG to close older Elasticsearch indices every 15 minutes',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 3, 24),
    tags=['elasticsearch', 'rotation']
)

ES_HOST = "http://elasticsearch-headless.airflow.svc.cluster.local"
ES_PORT = 9200
RETENTION_COUNT = 10  # Keep latest 10 indices open, close older ones

def get_es_client():
    """Create and return Elasticsearch client"""
    return Elasticsearch(
        hosts=[ES_HOST],
        port=ES_PORT,
        use_ssl=False,
        verify_certs=False
    )

def get_sorted_indices(es, pattern):
    """Fetch indices matching a pattern and sort them by numeric suffix"""
    all_indices = list(es.indices.get(index=pattern).keys())
    
    indexed_list = []
    for idx in all_indices:
        match = re.search(r'(\d+)$', idx)  # Extract numeric suffix
        if match:
            indexed_list.append((idx, int(match.group(1))))
    
    indexed_list.sort(key=lambda x: x[1])  # Sort by suffix

    return [i[0] for i in indexed_list]  # Return sorted index names

def close_old_indices(**context):
    """Close older indices while keeping the latest RETENTION_COUNT open"""
    es = get_es_client()
    patterns = ["index0-*", "index2-*"]  # Define index patterns

    for pattern in patterns:
        log.info(f"Processing indices matching pattern: {pattern}")

        try:
            sorted_indices = get_sorted_indices(es, pattern)

            if len(sorted_indices) <= RETENTION_COUNT:
                log.info(f"Retention met for {pattern}, no closing required.")
                continue

            indices_to_close = sorted_indices[:-RETENTION_COUNT]  # Keep only last N open

            for index in indices_to_close:
                try:
                    log.info(f"Closing index: {index}")
                    es.indices.close(index=index)
                except Exception as e:
                    log.error(f"Error closing index {index}: {str(e)}")

            log.info(f"Closed {len(indices_to_close)} indices for pattern {pattern}")

        except Exception as e:
            log.error(f"Error processing pattern {pattern}: {str(e)}")
            raise

close_indices_task = PythonOperator(
    task_id='close_old_indices',
    python_callable=close_old_indices,
    dag=dag
)

close_indices_task
