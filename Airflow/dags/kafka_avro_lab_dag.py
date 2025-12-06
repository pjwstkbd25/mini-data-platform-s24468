from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
TASKS_DIR = f"{AIRFLOW_HOME}/tasks"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="kafka_avro_lab",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["kafka", "schema-registry", "avro"],
) as dag:

    # 1) Czekamy aż Kafka nasłuchuje na porcie 9092
    #    używamy /dev/tcp, żeby nie polegać na nc
    wait_kafka = BashOperator(
        task_id="wait_kafka",
        bash_command=(
            'bash -lc "for i in {1..60}; do '
            '(echo > /dev/tcp/kafka/9092) >/dev/null 2>&1 && exit 0; '
            'sleep 2; '
            'done; exit 1"'
        ),
    )

    # 2) Czekamy aż Schema Registry odpowiada
    wait_schema_registry = BashOperator(
        task_id="wait_schema_registry",
        bash_command=(
            'bash -lc "for i in {1..60}; do '
            'curl -sf http://schema-registry:8081/subjects >/dev/null && exit 0; '
            'sleep 2; '
            'done; exit 1"'
        ),
    )

    # 3) Uruchamiamy Avro producera (plik w TASKS_DIR)
    run_avro_producer = BashOperator(
        task_id="run_avro_producer",
        bash_command=(
            f"cd {TASKS_DIR} && "
            "python producer_avro.py"
        ),
    )

    # 4) Uruchamiamy Avro consumera batchowego (też w TASKS_DIR)
    run_avro_consumer = BashOperator(
        task_id="run_avro_consumer",
        bash_command=(
            f"cd {TASKS_DIR} && "
            "python consumer_avro_batch.py"
        ),
    )

    wait_kafka >> wait_schema_registry >> run_avro_producer >> run_avro_consumer
