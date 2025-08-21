import json
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
from process_sales_ftp_to_pg.libs.upload import PgSalesHook


DAG_ID = "process_sales_ftp_to_pg"
SCHEDULE = '30 8,15 * * 1-5'


def get_local_tmp_dir_path() -> str:
    return os.path.join(Variable.get('tmp_dir_path'), 'sales')


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule=SCHEDULE,
    catchup=False,
    tags=['sales', 'ftp', 'postgresql']
) as dag:

    @task
    def download_task() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        local_dp = get_local_tmp_dir_path()
        os.makedirs(local_dp, exist_ok=True)

        sftp_hook = SFTPHook(Variable.get("sales_sftp_conn_id", default="sftp_1c"))
        remote_fp = Variable.get("sales_sftp_path")
        if not remote_fp:
            raise AirflowException("Airflow Variable 'sales_sftp_path' is not set")

        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales.txt")
        try:
            sftp_hook.retrieve_file(remote_fp, local_fp)
            logging.info("Downloaded %s to %s", remote_fp, local_fp)
        except FileNotFoundError:
            raise AirflowException(f"File not found on SFTP: {remote_fp}")

        return local_fp

    @task
    def transform_task(local_fp: str) -> str:
        df = pd.read_csv(local_fp, sep='\t')
        df_transformed = transform_sales_df(df)

        local_dp = get_local_tmp_dir_path()
        out_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_transformed.csv")
        df_transformed.to_csv(out_fp, index=False)
        logging.info("Transformed file saved to %s", out_fp)
        return out_fp

    @task
    def upload_task(transformed_fp: str):
        pg_hook = PgSalesHook(pg_conn_id=Variable.get("sales_pg_conn_id"))
        pg_hook.truncate_and_copy(transformed_fp)
        pg_hook.call_raw_to_ns()
        logging.info("Upload and procedure call complete.")

    @teardown
    def cleanup_task(local_fp: str, transformed_fp: str):
        files_to_delete = []
        if transformed_fp and os.path.exists(transformed_fp):
            files_to_delete.append(transformed_fp)
        for fp in files_to_delete:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("File %s removed.", fp)

    downloaded = download_task()
    transformed = transform_task(downloaded)
    uploaded = upload_task(transformed)

    uploaded >> cleanup_task(downloaded, transformed)


