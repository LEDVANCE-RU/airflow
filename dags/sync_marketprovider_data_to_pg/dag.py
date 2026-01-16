import json
from datetime import datetime

from airflow import DAG
from airflow.sdk import task, Variable

from constants import TZ_MSK, TZ_UTC


def _get_mp_api_token():
    return Variable.get('marketprovider_api_token')


def _get_mp_product_category_ids():
    value = Variable.get('marketprovider_product_category_ids')
    return json.loads(value)


with DAG(
    dag_id="sync_marketprovider_to_pg",
    start_date=datetime(2026, 1, 1, tzinfo=TZ_MSK),
    schedule='0 2,14 * * *',
    catchup=False,
) as dag:
    @task
    def get_current_datetime_task() -> str:
        return datetime.now(TZ_UTC).isoformat()

    @task
    def sync_categories_task():
        from sync_marketprovider_data_to_pg.libs.sync import sync_categories

        token = _get_mp_api_token()
        sync_categories(token)

    @task
    def sync_products_task():
        from sync_marketprovider_data_to_pg.libs.sync import sync_products

        token = _get_mp_api_token()
        category_ids = _get_mp_product_category_ids()
        sync_products(token, category_ids)

    @task
    def write_last_sync_datetime_task(dt: str):
        from db_model.db_broker import DbBroker
        from sync_marketprovider_data_to_pg.libs.constants import LAST_SYNC_KEY

        db_broker = DbBroker()
        db_broker.set_runtime_state(LAST_SYNC_KEY, datetime.fromisoformat(dt), commit=True)

    dt = get_current_datetime_task()
    dt >> sync_categories_task() >> sync_products_task() >> write_last_sync_datetime_task(dt)
