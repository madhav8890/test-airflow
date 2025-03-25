from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Email Configurations
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "madhav.prajapati@solvei8.com"
SENDER_PASSWORD = "your_password"  # Store in Airflow Secrets!
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"
FILE_NAME = "example.csv"


def create_dummy_file(**kwargs):
    """Generates CSV content and stores it in XCom."""
    file_content = "id,name,age\n1,John Doe,30\n2,Jane Doe,25"
    kwargs["ti"].xcom_push(key="file_content", value=file_content)
    print("File content stored in XCom")


def send_email(**kwargs):
    """Sends an email with the CSV content as an attachment."""
    ti = kwargs["ti"]
    file_content = ti.xcom_pull(task_ids="create_file", key="file_content")
    
    if not file_content:
        raise ValueError("File content not found in XCom!")

    message = MIMEMultipart()
    message["Subject"] = "Email with Attachment"
    message["From"] = SENDER_EMAIL
    message["To"] = RECIPIENT_EMAIL

    # Attach email body
    body_part = MIMEText("This is the body of the email.")
    message.attach(body_part)

    # Attach file from XCom
    attachment = MIMEApplication(file_content.encode(), Name=FILE_NAME)
    message.attach(attachment)

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
    provide_context=True,
    dag=dag
)

task2 = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    provide_context=True,
    dag=dag
)

task1 >> task2

