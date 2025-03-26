from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from kubernetes.client import models as k8s

# Email Configuration (Use Airflow Secrets in production)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "madhav.prajapati@solvei8.com"
SENDER_PASSWORD = "your-app-password"  # Use Airflow Secrets
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"

# Common PVC pod override configuration
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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "pvc_email_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def generate_file():
    """Generate a CSV file and store it in the PVC-mounted directory."""
    file_path = "/shared_data/example.csv"
    file_content = "id,name,age\n1,John Doe,30\n2,Jane Doe,25"
    
    with open(file_path, "w") as file:
        file.write(file_content)
    
    print(f"File generated at: {file_path}")

def send_email():
    """Send an email with the file attached from PVC."""
    file_path = "/shared_data/example.csv"

    message = MIMEMultipart()
    message["Subject"] = "Email with Attachment"
    message["From"] = SENDER_EMAIL
    message["To"] = RECIPIENT_EMAIL

    # Attach body
    message.attach(MIMEText("Here is the attached file."))

    # Attach file
    with open(file_path, "rb") as file:
        message.attach(MIMEApplication(file.read(), Name="example.csv"))

    # Send email
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, message.as_string())

    print("Email sent successfully!")

# Task 1: Generate file in PVC
generate_file_task = PythonOperator(
    task_id="generate_file",
    python_callable=generate_file,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 2: Send email with the file as an attachment
send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Define task dependencies
generate_file_task >> send_email_task

