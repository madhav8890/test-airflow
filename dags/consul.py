from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

CONSUL_URL = "http://consul-consul-server.airflow.svc.cluster.local:8500/v1/kv/?recurse"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 27),
    "retries": 1,
}

with DAG(
    "fetch_consul_kv",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_kv = BashOperator(
        task_id="fetch_consul_kv",
        bash_command=f"curl -s {CONSUL_URL} | jq '.'",
    )

    fetch_kv

