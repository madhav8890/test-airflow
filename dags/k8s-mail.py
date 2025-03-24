import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_manifest import KubernetesManifestOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.models import Variable

# Define PVC name and shared volume path
PVC_NAME = "airflow-dynamic-pvc"
MOUNT_PATH = "/mnt/airflow_shared"
FILE_PATH = f"{MOUNT_PATH}/example.csv"

# Fetch SMTP credentials from Airflow Variables (set these in Airflow UI)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = Variable.get("smtp_sender_email")  # Store in Airflow Variables
SENDER_PASSWORD = Variable.get("smtp_sender_password")  # Store securely
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"

# Define volume mount configuration
volume_mount = VolumeMount(
    name=PVC_NAME,
    mount_path=MOUNT_PATH,
    sub_path=None,
    read_only=False,
)

# Define volume configuration
volume = Volume(
    name=PVC_NAME,
    configs={"persistentVolumeClaim": {"claimName": PVC_NAME}},
)

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "email_with_pvc",
    default_args=default_args,
    description="Create PVC, generate file, and send email using Kubernetes pods",
    schedule_interval=None,
    start_date=datetime(2025, 3, 24),
    catchup=False,
    tags=["k8s", "pvc", "email"],
)

# Task 1: Create PersistentVolumeClaim (PVC)
create_pvc = KubernetesManifestOperator(
    task_id="create_pvc",
    namespace="airflow",
    manifest={
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": PVC_NAME, "namespace": "airflow"},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
        },
    },
    dag=dag,
)

# Task 2: Create File in PVC
create_file = KubernetesPodOperator(
    task_id="create_file",
    name="create-file-pod",
    namespace="airflow",
    image="python:3.9-slim",
    cmds=["bash", "-c"],
    arguments=[
        f"echo 'id,name,age\n1,John Doe,30\n2,Jane Doe,25' > {FILE_PATH} && ls -l {FILE_PATH}"
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    dag=dag,
)

# Task 3: Send Email with File from PVC
send_email = KubernetesPodOperator(
    task_id="send_email",
    name="send-email-pod",
    namespace="airflow",
    image="python:3.9-slim",
    cmds=["python", "-c"],
    arguments=[
        f"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

SMTP_SERVER = "{SMTP_SERVER}"
SMTP_PORT = {SMTP_PORT}
SENDER_EMAIL = "{SENDER_EMAIL}"
SENDER_PASSWORD = "{SENDER_PASSWORD}"
RECIPIENT_EMAIL = "{RECIPIENT_EMAIL}"
FILE_PATH = "{FILE_PATH}"

# Create email message
message = MIMEMultipart()
message['Subject'] = 'Email with Attachment'
message['From'] = SENDER_EMAIL
message['To'] = RECIPIENT_EMAIL

# Add body text
body_part = MIMEText('This is the body of the email.')
message.attach(body_part)

# Attach file
with open(FILE_PATH, 'rb') as file:
    message.attach(MIMEApplication(file.read(), Name='example.csv'))

# Send email
with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
    server.login(SENDER_EMAIL, SENDER_PASSWORD)
    server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, message.as_string())

print('Email sent successfully!')
"""
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    dag=dag,
)

# Set task dependencies
create_pvc >> create_file >> send_email

