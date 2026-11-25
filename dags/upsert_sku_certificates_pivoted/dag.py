from airflow import DAG
from airflow.sdk import task
from datetime import datetime

from constants import TZ_MSK

with DAG(
    dag_id="upsert_sku_certificates_pivoted",
    start_date=datetime(2026, 11, 1, tzinfo=TZ_MSK),
    schedule='30 1 * * *',
    catchup=False,
) as dag:
    @task
    def upsert_task():
        from db_model.db_broker import DbBroker

        with (DbBroker() as db_broker):
            db_broker.upsert_pivoted_sku_last_certificates()

    upsert_task()
