import logging
import os
import uuid

from airflow import DAG
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK


with DAG(
    dag_id="update_packages_data",
    start_date=datetime(2026, 11, 1, tzinfo=TZ_MSK),
    schedule='0 2 * * *',
    catchup=False,
) as dag:
    @task
    def transform_task() -> str:
        from update_packages_data.libs.transform import process_data

        local_dp = os.path.join(Variable.get('tmp_dir_path'), 'packages')
        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_packages.xlsx")
        process_data(local_fp)
        return local_fp

    @task
    def upload_task(local_fp: str):
        from hooks.webdav import WebDAVHook

        webdav_hook = WebDAVHook('webdav_sharepoint_root')
        webdav_client = webdav_hook.get_conn()

        remote_fp = os.path.join(Variable.get('packages_sp_out_dir'), "packages.xlsx")
        webdav_client.upload(remote_fp, local_fp)

        return True

    @teardown
    def cleanup_task(local_fp: str):
        os.remove(local_fp)
        logging.info("Temporary file %s removed.", local_fp)

    local_fp = transform_task()
    uploaded = upload_task(local_fp)

    uploaded >> cleanup_task(local_fp)
