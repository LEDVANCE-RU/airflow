import json
from datetime import datetime

from airflow import DAG
from airflow.sdk import task, Variable

from constants import TZ_MSK


with DAG(
    dag_id="sync_natcat_data_to_pg",
    start_date=datetime(2026, 1, 1, tzinfo=TZ_MSK),
    schedule='0 8,13,18 * * *',
    catchup=False,
) as dag:
    @task
    def sync_products_task():
        from sync_natcat_data_to_pg.libs.sync import sync_products

        api_key = _get_nc_api_key()
        owner_inn = _get_nc_owner_inn()
        sync_products(api_key, owner_inn)

    sync_products_task()


def _get_nc_api_key():
    return Variable.get('natcat_api_key')


def _get_nc_owner_inn():
    value = Variable.get('natcat_owner_inn')
    return json.loads(value)