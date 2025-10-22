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
    def retrieve_task() -> str | None:
        from notify_about_retired_stock.libs.retrieve import get_zeroed_stock_with_siblings

        df = get_zeroed_stock_with_siblings()
        if df is None or df.empty:
            logging.info('Обнуление стоков не обнаружено.')
            return None
        local_dp = get_tmp_local_dir_path()
        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}.parquet")
        os.makedirs(local_dp, exist_ok=True)
        df.to_parquet(local_fp)
        return local_fp

    @task.short_circuit
    def check_retired_stock_found_task(df_fp: str | None) -> bool:
        return df_fp is not None

    @task
    def transform_task(df_fp: str) -> str:
        from notify_about_retired_stock.libs.transform import save_to_excel, transform

        out_dp = get_tmp_local_dir_path()
        out_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}.xlsx")
        df = transform(df_fp)
        save_to_excel(df, out_fp)
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
    is_retired_stock_found = check_retired_stock_found_task(df_fp)
    out_fp = transform_task(df_fp)
    notified = notify_task(out_fp)

    df_fp >> is_retired_stock_found >> [out_fp, notified]
    (df_fp, out_fp, notified) >> cleanup_task([df_fp, out_fp])
