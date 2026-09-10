"""
Скачивает коммерческий отчёт по продажам с портала partner.lemanapro.ru
и загружает его в retail.lemana_sales (Postgres).

Портал блокирует автоматический логин headless-браузером, поэтому DAG
сам никогда не логинится - он переиспользует сессию (cookies +
localStorage), заранее сохранённую вручную в Airflow Variable
'lemana_portal_session_state' через refresh_session.py. Когда сессия
истекает, download_task падает с понятной ошибкой - нужно запустить
refresh_session.py и обновить Variable.

Необходимые объекты Airflow (создаются заранее, не через код):
  - Variable 'lemana_portal_session_state' - JSON сессии от refresh_session.py.
  - Variable 'tmp_dir_path' - уже используется другими DAG-ами репозитория.
  - Connection 'pg_prod' - уже используется другими DAG-ами репозитория.
"""
import logging
import os
import uuid
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable

from constants import TZ_MSK

with DAG(
    dag_id="lemana_sales_report",
    start_date=datetime(2025, 9, 1, tzinfo=TZ_MSK),
    schedule="0 7 * * *",
    catchup=False,
    tags=["lemana", "sales", "postgresql"],
) as dag:

    def get_local_tmp_dir_path():
        return os.path.join(Variable.get("tmp_dir_path"), "lemana_sales_report")

    @task
    def download_task() -> str:
        from lemana_sales_report.libs.report_downloader import download_report, SessionExpiredError

        local_dp = get_local_tmp_dir_path()
        os.makedirs(local_dp, exist_ok=True)
        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_lemana_sales.csv")

        try:
            download_report(local_fp)
        except SessionExpiredError as e:
            raise AirflowException(
                f"Сессия partner.lemanapro.ru недействительна или истекла: {e} "
                "Запустите dags/lemana_sales_report/refresh_session.py и обновите "
                "Airflow Variable 'lemana_portal_session_state'."
            )

        logging.info("Report downloaded to %s", local_fp)
        return local_fp

    @task
    def transform_task(local_fp: str) -> str:
        from lemana_sales_report.libs.transform import transform

        df = transform(local_fp)

        local_dp = get_local_tmp_dir_path()
        out_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_lemana_sales.parquet")
        df.to_parquet(out_fp, index=False, engine="pyarrow")
        logging.info("Transformed %d rows to %s", len(df), out_fp)
        return out_fp

    @task
    def upload_task(transformed_fp: str):
        from lemana_sales_report.libs.upload import PgLemanaSalesHook

        df = pd.read_parquet(transformed_fp)
        pg_hook = PgLemanaSalesHook(pg_conn_id="pg_prod")
        inserted = pg_hook.replace_months(df)
        logging.info("Loaded %d rows into retail.lemana_sales", inserted)

    @teardown
    def cleanup_local_files_task(local_fp: str, transformed_fp: str):
        for fp in (local_fp, transformed_fp):
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("Local file removed: %s", fp)

    downloaded = download_task()
    transformed = transform_task(downloaded)
    uploaded = upload_task(transformed)

    uploaded >> cleanup_local_files_task(downloaded, transformed)
