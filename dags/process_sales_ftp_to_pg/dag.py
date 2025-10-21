import csv
import logging
import os
import uuid

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable
from hooks.webdav import WebDAVHook
from datetime import datetime, timedelta

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

    def get_sales_export_sp_out_dir():
        return Variable.get("sales_export_sp_out_dir")

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
        out_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_transformed.parquet")
        df_transformed[SalesFieldsMap.dest_columns()].to_parquet(out_fp, index=False, engine='pyarrow')
        logging.info("Transformed file saved to %s", out_fp)
        return out_fp

    @task
    def upload_task(transformed_fp: str):
        df = pd.read_parquet(transformed_fp)
        
        local_dp = get_local_tmp_dir_path()
        temp_csv_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_for_pg.csv")
        df.to_csv(temp_csv_fp,
                  index=False,
                  encoding='utf-8',
                  sep=',',
                  quotechar='"',
                  quoting=csv.QUOTE_MINIMAL,
                  columns=SalesFieldsMap.dest_columns())
        
        try:
            pg_hook = PgSalesHook(pg_conn_id='pg_prod')
            pg_hook.truncate_and_copy(temp_csv_fp)
            pg_hook.call_raw_to_ns()
            logging.info("Upload complete.")
        finally:
            if os.path.exists(temp_csv_fp):
                os.remove(temp_csv_fp)

    @task
    def save_to_sharepoint_task(transformed_fp: str):
        if not transformed_fp or not os.path.exists(transformed_fp):
            logging.warning("Transformed file not found, skipping SharePoint save.")
            return

        df = pd.read_parquet(transformed_fp)

        local_dp = get_local_tmp_dir_path()
        temp_excel_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_sp.xlsx")
        df.to_excel(temp_excel_fp, index=False, engine='openpyxl')

        webdav_hook = WebDAVHook('webdav_sharepoint_root')
        webdav_client = webdav_hook.get_conn()

        current_date = datetime.now().strftime('%d.%m.%y')
        filename = f"sales_{current_date}.xlsx"
        sales_export_sp_out_dir = get_sales_export_sp_out_dir()
        remote_fp = os.path.join(sales_export_sp_out_dir, filename)

        try:
            webdav_client.upload(remote_fp, temp_excel_fp)
            logging.info("Sales file saved to SharePoint: %s", webdav_hook.get_full_path(remote_fp))
        finally:
            if os.path.exists(temp_excel_fp):
                os.remove(temp_excel_fp)

    @task
    def cleanup_sharepoint_history_task():
        webdav_hook = WebDAVHook('webdav_sharepoint_root')
        webdav_client = webdav_hook.get_conn()

        sales_export_sp_out_dir = get_sales_export_sp_out_dir()

        try:
            files = webdav_client.list(sales_export_sp_out_dir)
        except Exception as e:
            logging.warning("Failed to list SharePoint directory: %s", e)
            return

        cutoff_date = datetime.now() - timedelta(days=14)
        deleted_count = 0

        for file in files:
            if not file.startswith('sales_') or not file.endswith('.xlsx'):
                continue

            try:
                date_str = file.replace('sales_', '').replace('.xlsx', '')
                file_date = datetime.strptime(date_str, '%d.%m.%y')

                if file_date < cutoff_date:
                    remote_fp = os.path.join(sales_export_sp_out_dir, file)
                    webdav_client.clean(remote_fp)
                    deleted_count += 1
                    logging.info("Deleted old SharePoint file: %s (date: %s)", file, file_date.strftime('%d.%m.%Y'))
            except Exception as e:
                logging.warning("Failed to process file %s: %s", file, e)

        logging.info("SharePoint history cleanup complete. Deleted %d old file(s).", deleted_count)

    @teardown
    def cleanup_local_files_task(local_fp: str, transformed_fp: str):
        files_to_remove = [local_fp, transformed_fp]
        for fp in files_to_remove:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("Local file removed: %s", fp)

    downloaded = download_task()
    transformed = transform_task(downloaded)
    uploaded = upload_task(transformed)
    sp_saved = save_to_sharepoint_task(transformed)
    sp_cleanup = cleanup_sharepoint_history_task()

    uploaded >> sp_saved >> sp_cleanup >> cleanup_local_files_task(downloaded, transformed)


