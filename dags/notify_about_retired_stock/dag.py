import logging
import os
import uuid

from airflow import DAG
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK


def get_tmp_local_dir_path():
    return os.path.join(Variable.get('tmp_dir_path'), 'retired_stock')


with DAG(
    dag_id="notify_about_retired_stock",
    start_date=datetime(2025, 1, 1, tzinfo=TZ_MSK),
    schedule='30 1 * * *',
    catchup=False,
) as dag:
    @task
    def retrieve_task() -> str:
        from notify_about_retired_stock.libs.retrieve import get_zeroed_stock_with_siblings

        df = get_zeroed_stock_with_siblings()
        local_dp = get_tmp_local_dir_path()
        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}.parquet")
        df.to_parquet(local_fp)
        return local_fp

    @task
    def transform_task(df_fp: str) -> str:
        from notify_about_retired_stock.libs.transform import transform

        out_dp = get_tmp_local_dir_path()
        out_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}.xlsx")
        transform(df_fp, out_fp)
        return out_fp

    @task
    def notify_task(out_fp: str):
        from notify_about_retired_stock.libs.send import send_by_email

        dt_str = datetime.now(tz=TZ_MSK).strftime('%Y%m%d_%H%M%S')
        send_by_email(out_fp, f'Обнуление_остатков_{dt_str}.xlsx')

    @teardown
    def cleanup_task(filepaths: list[str]):
        for fp in filepaths:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("File %s removed.", fp)

    df_fp = retrieve_task()
    out_fp = transform_task(df_fp)
    notified = notify_task(out_fp)

    (df_fp, out_fp, notified) >> cleanup_task([df_fp, out_fp])
