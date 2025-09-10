import json
import logging
import os
import uuid

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK


with DAG(
    dag_id="process_report_sources_to_pg",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='25 9 * * 1-5',
    catchup=False,
    tags=['reports', 'sources', 'postgresql']
) as dag:

    def get_local_tmp_dir_path():
        return os.path.join(Variable.get('tmp_dir_path'), 'report_sources')

    @task
    def download_task() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        local_dp = get_local_tmp_dir_path()
        os.makedirs(local_dp, exist_ok=True)

        sftp_hook = SFTPHook("sftp_1c")

        required_files = [
            '1C_master_data_AG',
            'MTD_report_AG',
            'LTM_report_AG',
            'STOCK_report_AG',
            '1C_IC_AG',
            '1C_EAN_AG',
            'PO_report_NEW_AG',
            'BO_report_AG',
            '1C_packing_AG'
        ]

        filenames = Variable.get("report_sources_sftp_filenames", default="{}", deserialize_json=True)

        missing = [name for name in required_files if not filenames.get(name)]
        if missing:
            raise AirflowException(f"Missing filenames: {', '.join(missing)}")

        files_to_download = {name: os.path.join("/", filenames[name]) for name in required_files}

        local_filepaths = {}
        failed_keys = []
        run_hex = uuid.uuid4().hex

        for key, remote_fp in files_to_download.items():
            if not remote_fp:
                logging.info("SFTP path for %s is not configured. Skipping.", key)
                continue
            ext = os.path.splitext(remote_fp)[1]
            local_fp = os.path.join(local_dp, f"{run_hex}_{key}{ext}")
            try:
                sftp_hook.retrieve_file(remote_fp, local_fp)
                local_filepaths[key] = local_fp
                logging.info("Downloaded %s to %s", remote_fp, local_fp)
            except Exception as e:
                failed_keys.append(key)
                logging.error("Failed to download file %s from SFTP: %s", remote_fp, e)

        if failed_keys:
            raise AirflowException(f"Not all files were downloaded. Missing: {failed_keys}")

        return json.dumps(local_filepaths)

    @task
    def transform_task(downloaded_files_json: str) -> str:
        from process_report_sources_to_pg.libs.transform import transform_report_sources

        local_dp = get_local_tmp_dir_path()
        transformed = transform_report_sources(downloaded_files_json, local_dp)
        logging.info("Transformation complete.")
        return transformed

    @task
    def upload_task(transformed_table_to_file_json: str):
        from process_report_sources_to_pg.libs.upload import PgReportSourcesHook

        pg_hook = PgReportSourcesHook(pg_conn_id='pg_prod')
        pg_hook.upload_data(transformed_table_to_file_json)
        logging.info("Upload complete.")

    @teardown
    def cleanup_task(downloaded_files_json: str, transformed_table_to_file_json: str):
        files_to_delete = []
        if downloaded_files_json:
            files_to_delete.extend(json.loads(downloaded_files_json).values())
        if transformed_table_to_file_json:
            files_to_delete.extend(json.loads(transformed_table_to_file_json).values())

        for fp in files_to_delete:
            if not fp or not os.path.exists(fp):
                continue
            try:
                os.remove(fp)
                logging.info("File %s removed.", fp)
            except Exception as e:
                logging.warning("Could not remove file %s: %s", fp, e)

    downloaded_files = download_task()
    transformed = transform_task(downloaded_files)
    uploaded = upload_task(transformed)

    uploaded >> cleanup_task(downloaded_files, transformed)


