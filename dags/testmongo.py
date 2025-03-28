from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define backup location
BACKUP_PATH = "/shared_data/mongo_backup"

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

# DAG definition
dag = DAG(
    "mongodb_backup",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mongodb', 'backup'],
    description='MongoDB backup DAG using mongodump'
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

# Define task dependencies
mongodump_task >> verify_backup_task
