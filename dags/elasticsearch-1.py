from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import os
from kubernetes.client import models as k8s

# Elasticsearch Configuration
ES_HOST = "http://elasticsearch-headless.airflow.svc.cluster.local:9200"
INDEX_FILE = "/shared_data/indices.json"

# Index Retention Policy
CLOSE_DAYS = 1   # Close indices older than 1 day
DELETE_DAYS = 2  # Delete indices older than 2 days

# Kubernetes PVC Pod Override
pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-data",
                        mount_path="/shared_data"
                    )
                ]
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="shared-data",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="airflow-shared-pvc"
                )
            )
        ]
    )
)

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "elasticsearch_index_management",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

def fetch_indices():
    """Fetch all Elasticsearch indices and save to a file."""
    print("[FETCH] Fetching indices from Elasticsearch...")
    
    try:
        response = requests.get(f"{ES_HOST}/_cat/indices?v&format=json", timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[FETCH] ERROR: Failed to fetch indices. Error: {e}")
        raise
    
    indices = response.json()
    print(f"[FETCH] Retrieved {len(indices)} indices from Elasticsearch.")

    with open(INDEX_FILE, "w") as f:
        json.dump(indices, f, indent=2)
    
    print(f"[FETCH] Saved indices data to {INDEX_FILE}")

def extract_index_date(index_name):
    """Extracts the date from an index name formatted as 'myapp-ops-YYYY-MM-DD'."""
    try:
        date_str = index_name.split("-")[-3:]  # Extract last three parts (YYYY-MM-DD)
        index_date = datetime.strptime("-".join(date_str), "%Y-%m-%d").date()
        return index_date
    except ValueError:
        print(f"[ERROR] Skipping {index_name}: Invalid date format")
        return None

def close_old_indices():
    """Close indices older than CLOSE_DAYS and not already closed."""
    print("[CLOSE] Checking if index file exists...")
    if not os.path.exists(INDEX_FILE):
        print(f"[CLOSE] ERROR: Indices file not found: {INDEX_FILE}")
        raise FileNotFoundError(f"Indices file not found: {INDEX_FILE}")

    with open(INDEX_FILE, "r") as f:
        indices = json.load(f)

    today = datetime.utcnow().date()
    
    for index in indices:
        index_name = index["index"]
        index_status = index["status"]  # Check if already closed
        index_date = extract_index_date(index_name)
        
        if not index_date:
            continue  # Skip if the date couldn't be extracted

        days_old = (today - index_date).days
        print(f"[CLOSE] Checking {index_name}: {days_old} days old (Status: {index_status})")

        if days_old >= CLOSE_DAYS and index_status == "open":
            print(f"[CLOSE] Closing index {index_name}...")
            try:
                response = requests.post(f"{ES_HOST}/{index_name}/_close", timeout=10)
                response.raise_for_status()
                print(f"[CLOSE] Successfully closed index: {index_name}")
            except requests.exceptions.RequestException as e:
                print(f"[CLOSE] ERROR: Failed to close {index_name}. Error: {e}")

def delete_old_indices():
    """Delete indices older than DELETE_DAYS."""
    print("[DELETE] Checking if index file exists...")
    if not os.path.exists(INDEX_FILE):
        print(f"[DELETE] ERROR: Indices file not found: {INDEX_FILE}")
        raise FileNotFoundError(f"Indices file not found: {INDEX_FILE}")

    with open(INDEX_FILE, "r") as f:
        indices = json.load(f)

    today = datetime.utcnow().date()
    
    for index in indices:
        index_name = index["index"]
        index_date = extract_index_date(index_name)

        if not index_date:
            continue  # Skip if the date couldn't be extracted

        days_old = (today - index_date).days
        print(f"[DELETE] Checking {index_name}: {days_old} days old")

        if days_old >= DELETE_DAYS:
            print(f"[DELETE] Deleting index {index_name}...")
            try:
                response = requests.delete(f"{ES_HOST}/{index_name}", timeout=10)
                response.raise_for_status()
                print(f"[DELETE] Successfully deleted index: {index_name}")
            except requests.exceptions.RequestException as e:
                print(f"[DELETE] ERROR: Failed to delete {index_name}. Error: {e}")

# Task 1: Fetch all indices
fetch_task = PythonOperator(
    task_id="fetch_indices",
    python_callable=fetch_indices,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 2: Close indices older than CLOSE_DAYS
close_task = PythonOperator(
    task_id="close_old_indices",
    python_callable=close_old_indices,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 3: Delete indices older than DELETE_DAYS
delete_task = PythonOperator(
    task_id="delete_old_indices",
    python_callable=delete_old_indices,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Define task dependencies
fetch_task >> close_task >> delete_task

