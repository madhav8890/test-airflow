import os
import smtplib
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Email Configurations
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "madhav.prajapati@solvei8.com"
SENDER_PASSWORD = "xgpf inyu uvfm gpqb"  # Warning: Use Airflow Secrets
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"
FILE_PATH = "/tmp/example.csv"


def create_dummy_file():
    """Creates a dummy CSV file."""
    with open(FILE_PATH, "w") as f:
        f.write("id,name,age\n1,John Doe,30\n2,Jane Doe,25")
    print("File created:", FILE_PATH)


def send_email():
    """Sends an email with the file attached."""
    message = MIMEMultipart()
    message["Subject"] = "Email with Attachment"
    message["From"] = SENDER_EMAIL
    message["To"] = RECIPIENT_EMAIL

    # Email body
    body_part = MIMEText("This is the body of the email.")
    message.attach(body_part)

    # Attach file
    with open(FILE_PATH, "rb") as file:
        message.attach(MIMEApplication(file.read(), Name="example.csv"))

    # Send email
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, message.as_string())

    print("Email sent successfully!")


# Airflow DAG definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 24),
    "catchup": False
}

dag = DAG(
    "email_with_attachment",
    default_args=default_args,
    schedule_interval=None
)

task1 = PythonOperator(
    task_id="create_file",
    python_callable=create_dummy_file,
    dag=dag
)

task2 = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    dag=dag
)

# Task dependencies
task1 >> task2

