import logging
import os
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK
from send_internal_stock_report.libs.sender import get_stock_report_df, send_report_by_email

with DAG(
    dag_id="send_internal_stock_report",
    start_date=datetime(2026, 1, 1, tzinfo=TZ_MSK),
    schedule='0 5 * * *',
    catchup=False,
    tags=['report', 'email'],
) as dag:
    
    @task
    def get_report_task() -> pd.DataFrame:
        return get_stock_report_df(pg_conn_id='pg_prod')

    @task
    def send_report_task(report_df: pd.DataFrame) -> str:
        recipients = Variable.get("internal_stock_report_emails", default=None, deserialize_json=True)
        if not recipients or not isinstance(recipients, dict):
            raise AirflowException("Airflow Variable 'internal_stock_report_emails' is not set or invalid.")

        tmp_dir = Variable.get('tmp_dir_path')
        return send_report_by_email(report_df, recipients, tmp_dir)

    @teardown
    def cleanup_task(filepath: str):
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info("Cleaned up temporary file: %s", filepath)

    report_data = get_report_task()
    tmp_fp = send_report_task(report_data)
    cleanup_task(tmp_fp)
