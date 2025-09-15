import json
import logging
import os
import uuid

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK
from process_md_1c_to_pg.libs.transform import transform_md_data
from process_md_1c_to_pg.libs.upload import PgMdHook

with DAG(
    dag_id="process_md_1c_to_pg",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='0 20 * * 1-5',
    catchup=False,
    tags=['1c', 'md', 'postgresql']
) as dag:
    
    def get_local_tmp_dir_path():
        return os.path.join(Variable.get('tmp_dir_path'), 'md_1c')

    @task
    def download_task() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        local_dp = get_local_tmp_dir_path()
        os.makedirs(local_dp, exist_ok=True)
        
        sftp_hook = SFTPHook("sftp_1c") 
        
        files_to_download = {
            "products": Variable.get("md_1c_products_sftp_path"),
            "price_list": Variable.get("md_1c_pricelist_sftp_path")
        }
        
        local_filepaths = {}
        failed_keys = []

        for key, remote_fp in files_to_download.items():
            if not remote_fp:
                logging.info("SFTP path for %s is not configured. Skipping.", key)
                continue
            local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_{os.path.basename(remote_fp)}")
            try:
                sftp_hook.retrieve_file(remote_fp, local_fp)
                local_filepaths[key] = local_fp
                logging.info("Downloaded %s to %s", remote_fp, local_fp)
            except Exception as e:
                failed_keys.append(key)
                logging.error("Failed to download file %s from SFTP: %s", remote_fp, e)
        
        # Fail the DAG if at least one expected file failed to download
        if failed_keys:
            raise AirflowException(f"Not all files were downloaded. Missing: {failed_keys}")

        return json.dumps(local_filepaths)

    @task
    def transform_task(downloaded_files_json: str) -> str:
        local_dp = get_local_tmp_dir_path()
        transformed_files = transform_md_data(downloaded_files_json, local_dp)
        logging.info("Transformation complete.")
        return transformed_files

    @task
    def upload_task(transformed_data_json: str):
        pg_hook = PgMdHook(pg_conn_id='pg_prod')
        pg_hook.upload_data(transformed_data_json)
        logging.info("Upload complete.")

    @teardown
    def cleanup_task(downloaded_files_json: str, transformed_files_json: str):
        files_to_delete = []
        if downloaded_files_json:
            files_to_delete.extend(json.loads(downloaded_files_json).values())
        if transformed_files_json:
            files_to_delete.extend(json.loads(transformed_files_json).values())

        for fp in files_to_delete:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("File %s removed.", fp)

    downloaded_files = download_task()
    transformed_files = transform_task(downloaded_files)
    uploaded = upload_task(transformed_files)
    
    uploaded >> cleanup_task(downloaded_files, transformed_files)
