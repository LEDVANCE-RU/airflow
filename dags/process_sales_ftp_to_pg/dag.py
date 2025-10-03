import json
import csv
import logging
import os
import uuid

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK
from process_sales_ftp_to_pg.libs.transform import transform_sales_df
from process_sales_ftp_to_pg.libs.mapping import SalesFieldsMap
from process_sales_ftp_to_pg.libs.upload import PgSalesHook


with DAG(
    dag_id="process_sales_ftp_to_pg",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='30 8,15 * * 1-5',
    catchup=False,
    tags=['sales', 'ftp', 'postgresql']
) as dag:

    def get_local_tmp_dir_path():
        return os.path.join(Variable.get('tmp_dir_path'), 'sales')

    @task
    def download_task() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        local_dp = get_local_tmp_dir_path()
        os.makedirs(local_dp, exist_ok=True)

        sftp_hook = SFTPHook("sftp_1c")
        remote_fp = Variable.get("sales_sftp_path")
        if not remote_fp:
            raise AirflowException("Airflow Variable 'sales_sftp_path' is not set")

        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales.xlsx")
        try:
            sftp_hook.retrieve_file(remote_fp, local_fp)
            logging.info("Downloaded %s to %s", remote_fp, local_fp)
        except Exception as e:
            raise AirflowException(f"Failed to download from SFTP: {remote_fp}. Error: {e}")

        return local_fp

    @task
    def transform_task(local_fp: str) -> str:
        df = pd.read_excel(
            local_fp,
            dtype={
                'Артикул': str,
                'Код клиента': str,
                'Заказ.Number': str,
                'Проект.Number': str,
                'Номер проекта': str,
                'Заказ.Номер проекта': str,
            }
        )
        df_transformed = transform_sales_df(df)

        local_dp = get_local_tmp_dir_path()
        out_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_transformed.csv")
        df_transformed.to_csv(out_fp,
                              index=False,
                              encoding='utf-8',
                              sep=',',
                              quotechar='"',
                              quoting=csv.QUOTE_MINIMAL,
                              columns=SalesFieldsMap.dest_columns())
        logging.info("Transformed file saved to %s", out_fp)
        return out_fp

    @task
    def upload_task(transformed_fp: str):
        pg_hook = PgSalesHook(pg_conn_id='pg_prod')
        pg_hook.truncate_and_copy(transformed_fp)
        pg_hook.call_raw_to_ns()
        logging.info("Upload complete.")

    @teardown
    def cleanup_task(local_fp: str, transformed_fp: str):
        if transformed_fp and os.path.exists(transformed_fp):
            os.remove(transformed_fp)
            logging.info("File %s removed.", transformed_fp)

    downloaded = download_task()
    transformed = transform_task(downloaded)
    uploaded = upload_task(transformed)

    uploaded >> cleanup_task(downloaded, transformed)


