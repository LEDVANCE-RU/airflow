import logging
import json
import os
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK
from test_send_stock_report.libs.sender import get_stock_report_df, send_report_by_email

with DAG(
    dag_id="test_send_stock_report",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='0 5 * * *',
    catchup=False,
    tags=['test', 'report', 'email'],
) as dag:
    
    @task
    def get_report_task() -> pd.DataFrame:
        return get_stock_report_df(pg_conn_id='pg_prod')

    @task
    def send_report_task(report_df: pd.DataFrame):
        recipients = Variable.get("test_emails", default=None, deserialize_json=True)
        if not recipients or not isinstance(recipients, dict):
            raise AirflowException("Airflow Variable 'test_emails' is not set or invalid.")

        tmp_dir = Variable.get('tmp_dir_path')
        send_report_by_email(report_df, recipients, tmp_dir)

    @teardown
    def cleanup_task():
        tmp_dir = Variable.get('tmp_dir_path')
        filepath = os.path.join(tmp_dir, f'АО ЛЕДВАНС остатки на {datetime.now().strftime("%d.%m.%Y")}.xlsx')
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info("Cleaned up temporary file: %s", filepath)

    report_data = get_report_task()
    send_report_task(report_data) >> cleanup_task()
