from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import logging

# DAG Configurations
ELASTICSEARCH_CONN_ID = "elasticsearch_http"  # Set up in Airflow Connections
ELASTICSEARCH_HOST = "http://10.0.13.154:9200"
S3_REPO = "test-s3"
SNAPSHOT_PREFIX = "es-incr-snap"
RETENTION_DAYS = 7

# Generate snapshot name
SNAPSHOT_NAME = f"{SNAPSHOT_PREFIX}-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

# Setup logging
logging.basicConfig(filename="/var/log/es-incremental-backup.log", level=logging.INFO)

def check_es_cluster_health():
    """Checks Elasticsearch cluster health and ensures it is 'green' or 'yellow'."""
    url = f"{ELASTICSEARCH_HOST}/_cluster/health"
    response = requests.get(url)
    data = response.json()

    cluster_status = data.get("status")
    if cluster_status not in ["green", "yellow"]:
        logging.error(f"Cluster health is {cluster_status}. Exiting.")
        raise Exception(f"Cluster health is {cluster_status}, not safe for snapshot.")

    logging.info(f"Cluster health is {cluster_status}. Proceeding with snapshot.")

def create_snapshot():
    """Creates an incremental snapshot and verifies its success."""
    url = f"{ELASTICSEARCH_HOST}/_snapshot/{S3_REPO}/{SNAPSHOT_NAME}?wait_for_completion=true"
    payload = {
        "indices": "*",
        "ignore_unavailable": True,
        "include_global_state": True
    }

    response = requests.put(url, headers={"Content-Type": "application/json"}, json=payload)
    data = response.json()
    
    snapshot_status = data.get("snapshot", {}).get("state", "FAILED")
    if snapshot_status != "SUCCESS":
        logging.error(f"Snapshot {SNAPSHOT_NAME} failed: {json.dumps(data, indent=2)}")
        raise Exception(f"Snapshot {SNAPSHOT_NAME} failed. Check logs.")

    logging.info(f"Snapshot {SNAPSHOT_NAME} completed successfully.")

def cleanup_old_snapshots():
    """Deletes snapshots older than the retention period."""
    url = f"{ELASTICSEARCH_HOST}/_snapshot/{S3_REPO}/_all"
    response = requests.get(url)
    snapshots = response.json().get("snapshots", [])

    retention_timestamp = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).timestamp()

    for snapshot in snapshots:
        if snapshot.get("end_time_in_millis", 0) / 1000 < retention_timestamp:
            snapshot_name = snapshot["snapshot"]
            delete_url = f"{ELASTICSEARCH_HOST}/_snapshot/{S3_REPO}/{snapshot_name}"
            del_response = requests.delete(delete_url)
            
            if del_response.status_code == 200:
                logging.info(f"Deleted old snapshot: {snapshot_name}")
            else:
                logging.error(f"Failed to delete snapshot {snapshot_name}: {del_response.text}")

# DAG Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    "elasticsearch_incremental_snapshot_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    check_es_health = PythonOperator(
        task_id="check_es_health",
        python_callable=check_es_cluster_health
    )

    create_es_snapshot = PythonOperator(
        task_id="create_es_snapshot",
        python_callable=create_snapshot
    )

    # cleanup_snapshots = PythonOperator(
    #     task_id="cleanup_snapshots",
    #     python_callable=cleanup_old_snapshots
    # )

    # DAG dependencies
    check_es_health >> create_es_snapshot 
