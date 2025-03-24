from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Email Configuration (Use Airflow Secrets in production)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SENDER_EMAIL = "madhav.prajapati@solvei8.com"
SENDER_PASSWORD = "your-app-password"  # Use Airflow Secrets
RECIPIENT_EMAIL = "madhav5mar2001@gmail.com"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "xcom_email_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def generate_file_content(**kwargs):
    """Generate file content and push it to XCom."""
    file_content = "id,name,age\n1,John Doe,30\n2,Jane Doe,25"
    kwargs['ti'].xcom_push(key="file_data", value=file_content)
    print("File content stored in XCom.")

def write_to_file(**kwargs):
    """Retrieve file content from XCom and write it to a file."""
    ti = kwargs["ti"]
    file_content = ti.xcom_pull(task_ids="generate_file", key="file_data")
    file_path = "/tmp/example.csv"
    
    with open(file_path, "w") as file:
        file.write(file_content)
    
    ti.xcom_push(key="file_path", value=file_path)
    print(f"File written to {file_path}")

def send_email(**kwargs):
    """Send an email with the file attached."""
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="write_file", key="file_path")
    
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

# Task 1: Store file content in XCom
generate_file_task = PythonOperator(
    task_id="generate_file",
    python_callable=generate_file_content,
    provide_context=True,
    dag=dag,
)

# Task 2: Write content from XCom to a file
write_file_task = PythonOperator(
    task_id="write_file",
    python_callable=write_to_file,
    provide_context=True,
    dag=dag,
)

# Task 3: Send email with the file as an attachment
send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
generate_file_task >> write_file_task >> send_email_task

