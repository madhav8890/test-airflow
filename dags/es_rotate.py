from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import logging

log = logging.getLogger(__name__)

PARENT_DAG_NAME = 'ELASTICSEARCH_INDEX_ROTATION'

# Email notification settings
email_group = []  # Update with your email list if needed

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_group,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    PARENT_DAG_NAME,
    default_args=default_args,
    description='DAG to rotate Elasticsearch indices based on retention policy',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 3, 24),
    tags=['prod', 'elasticsearch']
)

# Elasticsearch configuration
ES_HOST = "http://elasticsearch1.stage-k8s.strawmine.com"
ES_PORT = 9200

def get_es_client():
    """Create and return Elasticsearch client"""
    return Elasticsearch(
        hosts=[ES_HOST],
        port=ES_PORT,
        use_ssl=False,
        verify_certs=False
    )

def get_indices_to_delete(es, retention_days, pattern):
    """Get list of indices older than retention period"""
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    indices = es.indices.get(index=pattern)
    
    indices_to_delete = []
    for index in indices:
        try:
            index_date_str = index.split('-')[-1]
            index_date = datetime.strptime(index_date_str, '%Y.%m.%d')
            if index_date < cutoff_date:
                indices_to_delete.append(index)
        except (IndexError, ValueError) as e:
            log.warning(f"Could not parse date from index {index}: {str(e)}")
    
    return indices_to_delete

def rotate_indices(**context):
    """Main function to handle index rotation"""
    es = get_es_client()
    
    # Example retention config (modify as needed)
    rotation_config = [
        {"index_pattern": "index0-*", "retention_days": 30},
        {"index_pattern": "index2-*", "retention_days": 15}
    ]
    
    for config in rotation_config:
        pattern = config['index_pattern']
        retention_days = config['retention_days']
        
        log.info(f"Processing pattern: {pattern} with retention: {retention_days} days")
        
        try:
            indices_to_delete = get_indices_to_delete(es, retention_days, pattern)
            
            if not indices_to_delete:
                log.info(f"No indices found matching pattern {pattern} older than {retention_days} days")
                continue
                
            for index in indices_to_delete:
                try:
                    log.info(f"Deleting index: {index}")
                    es.indices.delete(index=index)
                except Exception as e:
                    log.error(f"Error deleting index {index}: {str(e)}")
                    
            log.info(f"Deleted {len(indices_to_delete)} indices for pattern {pattern}")
            
        except Exception as e:
            log.error(f"Error processing pattern {pattern}: {str(e)}")
            raise

def check_indices_status(**context):
    """Check and report indices status"""
    es = get_es_client()
    
    try:
        health = es.cluster.health()
        log.info(f"Cluster health: {health['status']}")
        
        stats = es.indices.stats()
        total_indices = len(stats['indices'])
        total_size = stats['_all']['total']['store']['size_in_bytes'] / (1024 * 1024 * 1024)  # Convert to GB
        
        log.info(f"Total indices: {total_indices}")
        log.info(f"Total size: {total_size:.2f} GB")
        
        return {
            'cluster_health': health['status'],
            'total_indices': total_indices,
            'total_size_gb': round(total_size, 2)
        }
    except Exception as e:
        log.error(f"Error checking indices status: {str(e)}")
        raise

# Define tasks
rotate_indices_task = PythonOperator(
    task_id='rotate_indices',
    python_callable=rotate_indices,
    dag=dag
)

check_status_task = PythonOperator(
    task_id='check_indices_status',
    python_callable=check_indices_status,
    dag=dag
)

# Set task dependencies
rotate_indices_task >> check_status_task

