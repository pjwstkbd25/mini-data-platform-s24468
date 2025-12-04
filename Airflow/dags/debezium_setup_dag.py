from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="debezium_register_connector",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["debezium", "kafka"],
) as dag:

    wait_connect = BashOperator(
        task_id="wait_connect",
        bash_command='bash -lc "for i in {1..60}; do curl -sf http://connect:8083/ || true; if curl -sf http://connect:8083/; then exit 0; fi; sleep 2; done; exit 1"'
    )

    register = BashOperator(
        task_id="register_connector",
        bash_command='curl -v -X PUT -H "Content-Type: application/json" '
                     '--data-binary @/opt/airflow/secrets/debezium-pg.json '
                     'http://connect:8083/connectors/pg-source/config'
    )

    wait_connect >> register
