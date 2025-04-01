from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from datetime import datetime, timedelta

AWS_CONN_ID = "my_aws"  # Airflow connection ID
INSTANCE_IDS = ['i-0f5e996f090b18d2a']
RETENTION_DAYS = 6  # AMI retention period

def get_instance_name(ec2_hook, instance_id):
    """Fetch instance Name tag, defaulting to instance ID if not found."""
    ec2_client = ec2_hook.get_conn()
    tags = ec2_client.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0].get('Tags', [])
    return next((tag['Value'] for tag in tags if tag['Key'] == 'Name'), instance_id)

def create_ami():
    """Creates AMIs for EC2 instances and tags them."""
    ec2_hook = EC2Hook(aws_conn_id=AWS_CONN_ID, api_type="client_type")
    ec2_client = ec2_hook.get_conn()

    for instance_id in INSTANCE_IDS:
        instance_name = get_instance_name(ec2_hook, instance_id)
        ami_name = f"backup-{instance_name}-{datetime.utcnow().strftime('%Y-%m-%d')}"

        # Check if AMI already exists
        existing_amis = ec2_client.describe_images(
            Filters=[{'Name': 'name', 'Values': [ami_name]}, {'Name': 'state', 'Values': ['available']}],
            Owners=['self']
        )['Images']

        if existing_amis:
            print(f"AMI {ami_name} already exists. Skipping.")
            continue

        # Create AMI and tag it
        ami_id = ec2_client.create_image(InstanceId=instance_id, Name=ami_name, NoReboot=True)['ImageId']
        ec2_client.create_tags(Resources=[ami_id], Tags=[
            {'Key': 'Backup', 'Value': 'Daily'},
            {'Key': 'InstanceName', 'Value': instance_name}
        ])
        print(f"Created AMI: {ami_name} (AMI ID: {ami_id})")

def delete_old_amis():
    """Deletes AMIs older than the retention period."""
    ec2_hook = EC2Hook(aws_conn_id=AWS_CONN_ID, api_type="client_type")
    ec2_client = ec2_hook.get_conn()
    
    retention_time = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
    images = ec2_client.describe_images(Filters=[{'Name': 'tag:Backup', 'Values': ['Daily']}], Owners=['self'])['Images']

    for image in images:
        ami_id = image['ImageId']
        creation_time = datetime.strptime(image['CreationDate'], '%Y-%m-%dT%H:%M:%S.%fZ')

        if creation_time < retention_time:
            print(f"Deleting AMI: {ami_id} - Created At: {creation_time}")

            # Step 1: Deregister the AMI
            ec2_client.deregister_image(ImageId=ami_id)
            print(f"Deregistered AMI: {ami_id}")

            # Step 2: Delete associated snapshots
            for block in image.get('BlockDeviceMappings', []):
                if 'Ebs' in block and 'SnapshotId' in block['Ebs']:
                    snapshot_id = block['Ebs']['SnapshotId']
                    ec2_client.delete_snapshot(SnapshotId=snapshot_id)
                    print(f"Deleted Snapshot: {snapshot_id}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

with DAG(
    dag_id="ec2_ami_backup_dag",
    default_args=default_args,
    schedule_interval="0 * * * *",  # Run hourly
    catchup=False,
) as dag:

    create_ami_task = PythonOperator(
        task_id="create_ami",
        python_callable=create_ami,
    )

    delete_old_ami_task = PythonOperator(
        task_id="delete_old_amis",
        python_callable=delete_old_amis,
    )

    create_ami_task >> delete_old_ami_task
