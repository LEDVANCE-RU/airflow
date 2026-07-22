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
from process_sales_ic_ftp_to_pg.libs.transform import transform_sales_df
from process_sales_ic_ftp_to_pg.libs.on_delivery_transform import transform_on_delivery_df
from process_sales_ic_ftp_to_pg.libs.upload import PgSalesHook, PgOnDeliveryHook


with DAG(
    dag_id="process_sales_ic_ftp_to_pg",
    start_date=datetime(2026, 7, 20, tzinfo=TZ_MSK),
    schedule='30 8,15 * * 1-5',
    catchup=False,
    tags=['sales', 'ftp', 'postgresql']
) as dag:

    def get_local_tmp_dir_path(subdir: str = 'sales'):
        return os.path.join(Variable.get('tmp_dir_path'), subdir)

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
    def transform_task(local_fp: str) -> dict:
        df = pd.read_excel(
            local_fp,
            dtype={
                'Артикул': str,
                'Характеристика': str,
                'Контрагент.Партнер.Код': str,
            }
        )
        df_transformed = transform_sales_df(df)

        local_dp = get_local_tmp_dir_path()

        csv_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_for_pg.csv")
        df_transformed.to_csv(csv_fp,
                               index=False,
                               encoding='utf-8',
                               sep=',',
                               quotechar='"',
                               quoting=csv.QUOTE_MINIMAL)

        parquet_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_sales_transformed.parquet")
        df_transformed.to_parquet(parquet_fp, index=False, engine='pyarrow')

        logging.info("Transformed files saved: csv=%s, parquet=%s", csv_fp, parquet_fp)
        return {'csv_fp': csv_fp, 'parquet_fp': parquet_fp}

    @task
    def upload_task(csv_fp: str):
        pg_hook = PgSalesHook(pg_conn_id='pg_prod')
        pg_hook.load(csv_fp)
        logging.info("Upload complete.")

    @task
    def download_on_delivery_task() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        local_dp = get_local_tmp_dir_path('on_delivery')
        os.makedirs(local_dp, exist_ok=True)

        sftp_hook = SFTPHook("sftp_1c")
        remote_fp = "/on_delivery.csv"

        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_on_delivery.csv")
        try:
            sftp_hook.retrieve_file(remote_fp, local_fp)
            logging.info("Downloaded %s to %s", remote_fp, local_fp)
        except Exception as e:
            raise AirflowException(f"Failed to download from SFTP: {remote_fp}. Error: {e}")

        return local_fp

    @task
    def transform_on_delivery_task(local_fp: str) -> str:
        df = pd.read_csv(
            local_fp,
            sep=',',
            encoding='utf-8',
            dtype={
                'Клиент.Код': str,
                'Товары.Номенклатура.Артикул': str,
            }
        )
        df_transformed = transform_on_delivery_df(df)

        local_dp = get_local_tmp_dir_path('on_delivery')
        out_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_on_delivery_for_pg.csv")
        df_transformed.to_csv(out_fp,
                               index=False,
                               encoding='utf-8',
                               sep=',',
                               quotechar='"',
                               quoting=csv.QUOTE_MINIMAL)
        logging.info("On-delivery file transformed and saved to %s", out_fp)
        return out_fp

    @task
    def upload_on_delivery_task(csv_fp: str):
        pg_hook = PgOnDeliveryHook(pg_conn_id='pg_prod')
        pg_hook.load(csv_fp)
        logging.info("On-delivery upload complete.")

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
    def cleanup_local_files_task(local_fp: str, csv_fp: str, parquet_fp: str, od_local_fp: str, od_csv_fp: str):
        files_to_remove = [local_fp, csv_fp, parquet_fp, od_local_fp, od_csv_fp]
        for fp in files_to_remove:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("Local file removed: %s", fp)

    downloaded = download_task()
    transformed = transform_task(downloaded)
    uploaded = upload_task(transformed['csv_fp'])
    sp_saved = save_to_sharepoint_task(transformed['parquet_fp'])
    sp_cleanup = cleanup_sharepoint_history_task()

    od_downloaded = download_on_delivery_task()
    od_transformed = transform_on_delivery_task(od_downloaded)
    od_uploaded = upload_on_delivery_task(od_transformed)

    cleanup = cleanup_local_files_task(
        downloaded, transformed['csv_fp'], transformed['parquet_fp'], od_downloaded, od_transformed
    )
    uploaded >> sp_saved >> sp_cleanup >> cleanup
    od_uploaded >> cleanup


