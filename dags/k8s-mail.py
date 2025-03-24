import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable

# Fetch credentials from Airflow variables (Set these in Airflow UI under "Admin > Variables")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = Variable.get("smtp_sender_email")
SENDER_PASSWORD = Variable.get("smtp_sender_password")  # Store securely in Airflow
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"
FILE_PATH = "/tmp/example.csv"

# Define default arguments
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
    "email_with_attachment_k8s",
    default_args=default_args,
    description="Create a file and send it as an email using K8s pods",
    schedule_interval=None,
    start_date=datetime(2025, 3, 24),
    catchup=False,
    tags=["email", "k8s"],
)

# Task 1: Create Dummy File in a New K8s Pod
create_file = KubernetesPodOperator(
    task_id="create_file",
    name="create-file-pod",
    namespace="airflow",  # Change if your namespace is different
    image="python:3.9-slim",
    cmds=["bash", "-c"],
    arguments=[
        f"echo 'id,name,age\n1,John Doe,30\n2,Jane Doe,25' > {FILE_PATH} && ls -l {FILE_PATH}"
    ],
    get_logs=True,
    dag=dag,
)

# Task 2: Send Email with Attachment in a New K8s Pod
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
    get_logs=True,
    dag=dag,
)

# Task Dependencies
create_file >> send_email

