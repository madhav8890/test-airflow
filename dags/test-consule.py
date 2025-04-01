from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import base64
import os
import requests
from kubernetes.client import models as k8s

# Consul Configuration
CONSUL_ENDPOINT = "http://consul-consul-server.airflow.svc.cluster.local:8500/v1/kv"
TEST_FOLDER = "test/"
BACKUP_FOLDER = "backup/"
BACKUP_FILE = "/shared_data/consul_backup.json"
PROCESSED_FILE = "/shared_data/consul_backup_processed.json"

# S3 Configuration
S3_BUCKET_NAME = "airflow-test-backup-bucket"
S3_OBJECT_KEY = "kv-backup/consul_backup_processed.json"
S3_CONN_ID = "aws_s3_connection"  # Update with your Airflow S3 connection ID

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
    "consul_test_to_backup-1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def fetch_test_folder_kv():
    """Fetches all Consul KV pairs from the `test/` folder and saves them to a backup file."""
    response = requests.get(f"{CONSUL_ENDPOINT}/{TEST_FOLDER}?recurse")
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch KV data: {response.text}")

    kv_data = response.json()

    with open(BACKUP_FILE, "w") as f:
        json.dump(kv_data, f, indent=2)

    print(f"Backup saved to {BACKUP_FILE}")

def process_consul_backup():
    """Reads KV backup from PVC, decodes base64 values, and saves the processed file."""
    if not os.path.exists(BACKUP_FILE):
        raise FileNotFoundError(f"Backup file not found: {BACKUP_FILE}")

    with open(BACKUP_FILE, "r") as f:
        data = json.load(f)

    processed_data = []
    for entry in data:
        key = entry.get("Key")
        value = entry.get("Value")

        # Remove 'test/' prefix from the key
        new_key = key.replace(TEST_FOLDER, BACKUP_FOLDER, 1)

        if value:
            try:
                value_decoded = base64.b64decode(value).decode("utf-8")
            except Exception:
                value_decoded = value  # Keep original if decoding fails
        else:
            value_decoded = ""

        processed_data.append({"Key": new_key, "Value": value_decoded})

    with open(PROCESSED_FILE, "w") as f:
        json.dump(processed_data, f, indent=2)

    print("Processed KV backup saved.")

def upload_to_backup_folder():
    """Uploads processed KV data to the `backup/` folder in Consul."""
    if not os.path.exists(PROCESSED_FILE):
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}")

    with open(PROCESSED_FILE, "r") as f:
        data = json.load(f)

    for entry in data:
        key = entry["Key"]  # Already updated to `backup/`
        value = entry["Value"]
        url = f"{CONSUL_ENDPOINT}/{key}"

        response = requests.put(url, data=value)
        if response.status_code == 200:
            print(f"Successfully uploaded: {key}")
        else:
            print(f"Failed to upload {key}: {response.text}")

def upload_to_s3():
    """Uploads the processed KV backup to an S3 bucket using S3Hook."""
    if not os.path.exists(PROCESSED_FILE):
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_FILE}")

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    try:
        s3_hook.load_file(
            filename=PROCESSED_FILE,
            bucket_name=S3_BUCKET_NAME,
            key=S3_OBJECT_KEY,
            replace=True
        )
        print(f"Successfully uploaded {PROCESSED_FILE} to S3: s3://{S3_BUCKET_NAME}/{S3_OBJECT_KEY}")
    except Exception as e:
        raise Exception(f"Failed to upload to S3: {str(e)}")

# Task 1: Fetch KV pairs from `test/`
fetch_task = PythonOperator(
    task_id="fetch_test_folder_kv",
    python_callable=fetch_test_folder_kv,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 2: Process KV pairs and change folder from `test/` to `backup/`
process_task = PythonOperator(
    task_id="process_consul_backup",
    python_callable=process_consul_backup,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 3: Upload processed KV pairs to `backup/`
upload_task = PythonOperator(
    task_id="upload_to_backup_folder",
    python_callable=upload_to_backup_folder,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 4: Upload processed backup to S3
upload_s3_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Define task dependencies
fetch_task >> process_task >> upload_task >> upload_s3_task
