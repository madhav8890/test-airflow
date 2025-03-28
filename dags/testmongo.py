from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from airflow.hooks.S3_hook import S3Hook
import os

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define paths and S3 details
BACKUP_PATH = "/shared_data/mongo_backup"
S3_BUCKET = "mongo-backups-bucket"  # Replace with your S3 bucket name
S3_KEY_PREFIX = "mongodb-backups"

# Create proper Kubernetes volume and volume mount objects
volume = k8s.V1Volume(
    name="shared-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="airflow-shared-pvc"
    )
)

volume_mount = k8s.V1VolumeMount(
    name="shared-data",
    mount_path="/shared_data"
)

def upload_to_s3(**context):
    """Upload MongoDB backup to S3"""
    # Initialize S3 hook using the AWS connection
    s3_hook = S3Hook(aws_conn_id='')  # Use your AWS connection ID
    
    # Get current date for backup folder name
    backup_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    # Walk through the backup directory and upload all files
    for root, dirs, files in os.walk(BACKUP_PATH):
        for file in files:
            local_file_path = os.path.join(root, file)
            # Create S3 key maintaining directory structure
            relative_path = os.path.relpath(local_file_path, BACKUP_PATH)
            s3_key = f"{S3_KEY_PREFIX}/{backup_date}/{relative_path}"
            
            # Upload file to S3
            s3_hook.load_file(
                filename=local_file_path,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
            )
            print(f"Uploaded {local_file_path} to s3://{S3_BUCKET}/{s3_key}")

# DAG definition
dag = DAG(
    "mongodb_backup_s3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mongodb', 'backup', 's3'],
    description='MongoDB backup DAG using mongodump with S3 upload'
)

# Task 1: Run mongodump
mongodump_task = KubernetesPodOperator(
    task_id="run_mongodump",
    name="mongo-backup-job",
    namespace="airflow",
    image="mongo:4.4",
    cmds=["sh", "-c"],
    arguments=[
        f"mongodump --host=mongodb-service.airflow.svc.cluster.local --port=27017 --out={BACKUP_PATH}"
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    is_delete_operator_pod=True,
    dag=dag,
)

# Task 2: Verify backup
verify_backup_task = KubernetesPodOperator(
    task_id="verify_backup",
    name="verify-backup",
    namespace="airflow",
    image="busybox",
    cmds=["sh", "-c"],
    arguments=[
        f"ls -lah {BACKUP_PATH}"
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    is_delete_operator_pod=True,
    dag=dag,
)

# Task 3: Upload to S3
upload_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    executor_config={"pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    volume_mounts=[volume_mount]
                )
            ],
            volumes=[volume]
        )
    )},
    dag=dag,
)

# Define task dependencies
mongodump_task >> verify_backup_task >> upload_s3_task
