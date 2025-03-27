from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import base64
import os
import requests
from kubernetes.client import models as k8s

# Consul Configuration
CONSUL_ENDPOINT = "http://consul-consul-server.airflow.svc.cluster.local:8500/v1/kv"
BACKUP_FILE = "/shared_data/consul_backup.json"
PROCESSED_FILE = "/shared_data/consul_backup_processed.json"

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
    "consul_kv_backup",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def read_consul_backup():
    """Reads Consul KV backup from PVC, decodes base64 values, and saves the processed file."""
    if not os.path.exists(BACKUP_FILE):
        raise FileNotFoundError(f"Backup file not found: {BACKUP_FILE}")

    with open(BACKUP_FILE, "r") as f:
        data = json.load(f)

    processed_data = []
    for entry in data:
        key = entry.get("Key")
        value = entry.get("Value")

        if value:
            try:
                value_decoded = base64.b64decode(value).decode("utf-8")
            except Exception:
                value_decoded = value  # Keep original if decoding fails
        else:
            value_decoded = ""

        processed_data.append({"Key": key, "Value": value_decoded})

    with open(PROCESSED_FILE, "w") as f:
        json.dump(processed_data, f, indent=2)

    print("Processed KV backup saved.")

def upload_to_consul():
    """Uploads processed KV data back to Consul."""
    if not os.path.exists(PROCESSED_FILE):
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}")

    with open(PROCESSED_FILE, "r") as f:
        data = json.load(f)

    for entry in data:
        key = entry["Key"]
        value = entry["Value"]
        url = f"{CONSUL_ENDPOINT}/{key}"

        response = requests.put(url, data=value)
        if response.status_code == 200:
            print(f"Successfully uploaded: {key}")
        else:
            print(f"Failed to upload {key}: {response.text}")

# Task 1: Read and process Consul KV backup
read_task = PythonOperator(
    task_id="read_consul_backup",
    python_callable=read_consul_backup,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 2: Upload processed KV back to Consul
upload_task = PythonOperator(
    task_id="upload_to_consul",
    python_callable=upload_to_consul,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Define task dependencies
read_task >> upload_task
