import logging
import os

from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import task
from datetime import datetime, UTC

from airflow.utils.trigger_rule import TriggerRule

from constants import AIRFLOW_TMP_DIRPATH, TZ_MSK

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": 30,
}

with DAG(
    dag_id="upload_packages_sftp_1c_to_pg",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='30 1 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
) as dag:
    @task
    def download_task():
        local_dp = os.path.join(AIRFLOW_TMP_DIRPATH, 'packages')
        local_fp = os.path.join(local_dp, f"packages_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.xlsx")
        remote_fp = "/packages.xlsx"

        os.makedirs(local_dp, exist_ok=True)

        sftp_hook = SFTPHook("sftp_1c")
        sftp_hook.retrieve_file(remote_fp, local_fp)
        return local_fp

    @task
    def transform_task(source_fp: str):
        from upload_packages_sftp_1c_to_pg.transform import transform
        transformed_fp = f"{source_fp}_t.csv"
        transform(source_fp, transformed_fp)
        return transformed_fp

    @task
    def upload_task(transformed_fp: str):
        from upload_packages_sftp_1c_to_pg.upload import PgPackagesHook

        pg = PgPackagesHook('pg_prod', transformed_fp)
        pg.create_table()
        pg.clear_table()
        pg.import_data()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_task(fpaths: list[str]):
        for fp in fpaths:
            if os.path.exists(fp):
                os.remove(fp)
                logging.info("File %s removed.", fp)

    local_fp = download_task()
    transformed_fp = transform_task(local_fp)
    upload_task(transformed_fp) >> cleanup_task([local_fp, transformed_fp])
