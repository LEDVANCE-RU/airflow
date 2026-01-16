import logging
from datetime import datetime

from airflow import DAG
from airflow.sdk import task

from constants import TZ_MSK

with DAG(
    dag_id="sync_marketprovider_to_pg",
    start_date=datetime(2025, 1, 1, tzinfo=TZ_MSK),
    schedule='30 1 15 * *',
    catchup=False,
) as dag:
    @task
    def sync_categories_task() -> str | None:
        ...