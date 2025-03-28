from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 28),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define Shared Persistent Volume
SHARED_VOLUME_NAME = "shared-data"
PVC_NAME = "airflow-shared-pvc"
BACKUP_PATH = "/shared_data/mongo_backup_$(date +%Y%m%d_%H%M%S)"

# Common volume configuration
volume_config = {
    "name": SHARED_VOLUME_NAME,
    "persistentVolumeClaim": {"claimName": PVC_NAME},
}

volume_mount_config = {
    "name": SHARED_VOLUME_NAME,
    "mountPath": "/shared_data"
}

# DAG definition
dag = DAG(
    "mongodb_backup",
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['mongodb', 'backup'],
    description='MongoDB backup DAG using mongodump'
)

# Task 1: Create backup directory
init_backup_task = KubernetesPodOperator(
    task_id="init_backup_dir",
    name="init-backup-dir",
    namespace="airflow",
    image="busybox",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p /shared_data && echo 'Backup directory initialized'"
    ],
    volumes=[volume_config],
    volume_mounts=[volume_mount_config],
    is_delete_operator_pod=True,
    dag=dag,
)

# Task 2: KubernetesPodOperator for Mongodump
mongodump_task = KubernetesPodOperator(
    task_id="run_mongodump",
    name="mongo-backup-job",
    namespace="airflow",
    image="mongo:4.4",
    cmds=["sh", "-c"],
    arguments=[
        f"""
        mongodump \
          --host=mongodb-service.airflow.svc.cluster.local \
          --port=27017 \
          --out={BACKUP_PATH} && \
        echo 'Backup completed successfully!' || \
        (echo 'Backup failed!'; exit 1)
        """
    ],
    volumes=[volume_config],
    volume_mounts=[volume_mount_config],
    is_delete_operator_pod=True,
    dag=dag,
)

# Task 3: Verify backup and list files
verify_backup_task = KubernetesPodOperator(
    task_id="verify_backup",
    name="verify-backup",
    namespace="airflow",
    image="busybox",
    cmds=["sh", "-c"],
    arguments=[
        f"""
        if [ -d "{BACKUP_PATH}" ]; then
            echo "Backup directory exists"
            echo "Backup contents:"
            ls -lah {BACKUP_PATH}
            echo "Backup size:"
            du -sh {BACKUP_PATH}
        else
            echo "Backup directory not found!"
            exit 1
        fi
        """
    ],
    volumes=[volume_config],
    volume_mounts=[volume_mount_config],
    is_delete_operator_pod=True,
    dag=dag,
)

# Define task dependencies
init_backup_task >> mongodump_task >> verify_backup_task
