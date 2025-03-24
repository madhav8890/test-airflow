from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

# Kubernetes Config
NAMESPACE = "airflow"  # Change if needed
PVC_NAME = "airflow-shared-pvc"
MOUNT_PATH = "/mnt/shared"

# Define Volume and VolumeMount
volume_mount = VolumeMount(
    name=PVC_NAME, mount_path=MOUNT_PATH, sub_path=None, read_only=False
)
volume = Volume(
    name=PVC_NAME, configs={"persistentVolumeClaim": {"claimName": PVC_NAME}}
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "k8s_pvc_email_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Task 1: Create a file inside the shared PVC
create_file_pod = KubernetesPodOperator(
    task_id="create_file_pod",
    name="create-file-pod",
    namespace=NAMESPACE,
    image="python:3.9",
    cmds=["/bin/sh", "-c"],
    arguments=[
        f"echo 'id,name,age\n1,John Doe,30\n2,Jane Doe,25' > {MOUNT_PATH}/example.csv"
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    is_delete_operator_pod=True,
    dag=dag,
)

# Task 2: Read the file and send an email
send_email_pod = KubernetesPodOperator(
    task_id="send_email_pod",
    name="send-email-pod",
    namespace=NAMESPACE,
    image="python:3.9",
    cmds=["/bin/sh", "-c"],
    arguments=[
        f"""
        pip install smtplib &&
        python -c '
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "madhav.prajapati@solvei8.com"
SENDER_PASSWORD = "your-app-password"  # Use Airflow Secrets
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"
FILE_PATH = "{MOUNT_PATH}/example.csv"

# Prepare email
message = MIMEMultipart()
message["Subject"] = "Email with Attachment"
message["From"] = SENDER_EMAIL
message["To"] = RECIPIENT_EMAIL

# Attach body
message.attach(MIMEText("Here is the file."))

# Attach file
with open(FILE_PATH, "rb") as file:
    message.attach(MIMEApplication(file.read(), Name="example.csv"))

# Send email
with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
    server.login(SENDER_EMAIL, SENDER_PASSWORD)
    server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, message.as_string())

print("Email sent successfully!")
        '
        """
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    is_delete_operator_pod=True,
    dag=dag,
)

# Define dependencies
create_file_pod >> send_email_pod

