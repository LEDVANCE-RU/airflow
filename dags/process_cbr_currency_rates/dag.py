import json
import logging
import os

from airflow import DAG
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK
from process_cbr_currency_rates.libs.transform import transform_cbr_rates
from process_cbr_currency_rates.libs.upload import PgCbrHook


with DAG(
    dag_id="process_cbr_currency_rates",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='55 23 * * *',
    catchup=False,
    tags=['cbr', 'rates', 'postgresql']
) as dag:

    def get_local_tmp_dir_path():
        return os.path.join(Variable.get('tmp_dir_path'), 'cbr_rates')

    @task
    def transform_task() -> str:
        local_dp = get_local_tmp_dir_path()
        transformed_files = transform_cbr_rates(local_dp)
        logging.info("Transformation complete.")
        return transformed_files

    @task
    def upload_task(transformed_data_json: str):
        pg_hook = PgCbrHook(pg_conn_id='pg_prod')
        pg_hook.upload_rates(transformed_data_json)
        logging.info("Upload complete.")

    @teardown
    def cleanup_task(transformed_files_json: str):
        files_to_delete = []
        if transformed_files_json:
            tf = json.loads(transformed_files_json)
            if isinstance(tf, dict):
                files_to_delete.extend([fp for fp in tf.values() if isinstance(fp, str)])
        for fp in files_to_delete:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("File %s removed.", fp)

    transformed_files = transform_task()
    uploaded = upload_task(transformed_files)
    uploaded >> cleanup_task(transformed_files)


