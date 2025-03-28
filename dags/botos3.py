from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.dates import days_ago

# Fetch AWS credentials from Airflow connection
aws_hook = AwsBaseHook(aws_conn_id='my_aws', client_type='sts')
credentials = aws_hook.get_credentials()

# Define the DAG
dag = DAG(
    'list_ec2_instances_k8s',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# KubernetesPodOperator to run AWS CLI
list_instances_task = KubernetesPodOperator(
    task_id='list_instances',
    name='aws-cli-list-instances',
    namespace='default',  # Change namespace if needed
    image='amazon/aws-cli',  # AWS CLI official image
    cmds=["sh", "-c"],
    arguments=[
        "aws ec2 describe-instances --query 'Reservations[*].Instances[*].[InstanceId,State.Name]' --output table"
    ],
    env_vars={
        'AWS_ACCESS_KEY_ID': credentials.access_key,
        'AWS_SECRET_ACCESS_KEY': credentials.secret_key,
        'AWS_DEFAULT_REGION': 'us-east-1',  # Change as needed
    },
    dag=dag,
)

list_instances_task
