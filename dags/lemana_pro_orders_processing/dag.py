from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta

from constants import TZ_MSK
from db_model.db_broker import DbBroker


with DAG(
    dag_id="lemana_pro_orders_processing",
    start_date=datetime(2025, 1, 1, tzinfo=TZ_MSK),
    schedule='0 * * * *',
    catchup=False,
) as dag:
    @task
    def retrieve_task():
        from hooks.exchange import ExchangeHook
        from exchangelib import EWSDateTime, UTC

        from lemana_pro_orders_processing.libs.constants import SENDER, SUBJECT_PATTERN
        from lemana_pro_orders_processing.libs.order_parser import OrderParser

        exch_hook = ExchangeHook('exchange_sys_tech')
        exch_hook.get_conn()

        parser = OrderParser()
        orders = []
        now = EWSDateTime.now(UTC)
        for item in exch_hook.iter_inbox(
                sender=SENDER,
                subject__contains=SUBJECT_PATTERN,
                datetime_received__range=(now - timedelta(days=7), now)
        ):
            order = parser.parse(item.body, item.datetime_received)
            orders.append(order)
            with DbBroker() as db_broker:
                db_broker.insert_lemana_pro_order(order)
            item.move_to_trash()
