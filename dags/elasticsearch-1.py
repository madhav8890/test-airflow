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

# Kubernetes PVC pod override configuration
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
    response = requests.get(f"{ES_HOST}/_cat/indices?v&format=json")
    
    if response.status_code != 200:
        print(f"[FETCH] ERROR: Failed to fetch indices. Response: {response.text}")
        raise Exception(f"Failed to fetch indices: {response.text}")

    indices = response.json()
    print(f"[FETCH] Retrieved {len(indices)} indices from Elasticsearch.")

    with open(INDEX_FILE, "w") as f:
        json.dump(indices, f, indent=2)
    
    print(f"[FETCH] Saved indices data to {INDEX_FILE}")

def close_old_indices():
    """Close indices older than 1 day and not already closed."""
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
        try:
            date_str = index_name.split("-")[-1]  # Extract date from name
            index_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print(f"[CLOSE] Skipping {index_name}: Invalid date format")
            continue

        days_old = (today - index_date).days
        print(f"[CLOSE] Checking {index_name}: {days_old} days old (Status: {index_status})")

        if days_old > 1 and index_status == "open":
            print(f"[CLOSE] Closing index {index_name}...")
            response = requests.post(f"{ES_HOST}/{index_name}/_close")
            if response.status_code == 200:
                print(f"[CLOSE] Successfully closed index: {index_name}")
            else:
                print(f"[CLOSE] ERROR: Failed to close {index_name}. Response: {response.text}")

def delete_old_indices():
    """Delete indices older than 2 days."""
    print("[DELETE] Checking if index file exists...")
    if not os.path.exists(INDEX_FILE):
        print(f"[DELETE] ERROR: Indices file not found: {INDEX_FILE}")
        raise FileNotFoundError(f"Indices file not found: {INDEX_FILE}")

    with open(INDEX_FILE, "r") as f:
        indices = json.load(f)

    today = datetime.utcnow().date()
    
    for index in indices:
        index_name = index["index"]
        try:
            date_str = index_name.split("-")[-1]  # Extract date from name
            index_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print(f"[DELETE] Skipping {index_name}: Invalid date format")
            continue

        days_old = (today - index_date).days
        print(f"[DELETE] Checking {index_name}: {days_old} days old")

        if days_old > 2:
            print(f"[DELETE] Deleting index {index_name}...")
            response = requests.delete(f"{ES_HOST}/{index_name}")
            if response.status_code == 200:
                print(f"[DELETE] Successfully deleted index: {index_name}")
            else:
                print(f"[DELETE] ERROR: Failed to delete {index_name}. Response: {response.text}")

# Task 1: Fetch all indices
fetch_task = PythonOperator(
    task_id="fetch_indices",
    python_callable=fetch_indices,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 2: Close indices older than 1 day
close_task = PythonOperator(
    task_id="close_old_indices",
    python_callable=close_old_indices,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 3: Delete indices older than 2 days
delete_task = PythonOperator(
    task_id="delete_old_indices",
    python_callable=delete_old_indices,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Define task dependencies
fetch_task >> close_task >> delete_task

