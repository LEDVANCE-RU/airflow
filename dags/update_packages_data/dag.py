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

        local_fname = f'packages_{str(uuid.uuid4())}.xlsx'
        process_data(local_fname)
        return local_fname

    @task
    def upload_task(local_fname: str):
        from hooks.webdav import WebDAVHook

        webdav_hook = WebDAVHook('webdav_sharepoint_root')
        webdav_client = webdav_hook.get_conn()

        remote_fp = os.path.join(Variable.get('packages_sp_out_dir'), "packages.xlsx")
        webdav_client.upload(remote_fp, local_fname)

        return True

    @teardown
    def cleanup_task(local_fname: str):
        os.remove(local_fname)
        logging.info("Temporary file %s removed.", local_fname)

    local_fname = transform_task()
    uploaded = upload_task(local_fname)
    cleanup_task(local_fname)

    uploaded >> cleanup_task(local_fname)
