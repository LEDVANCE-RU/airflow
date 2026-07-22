from airflow.providers.postgres.hooks.postgres import PostgresHook
from process_sales_ic_ftp_to_pg.libs.constants import SALES_DELETE_SQL, SALES_INSERT_SQL
from process_sales_ic_ftp_to_pg.libs.mapping import SalesFieldsMap
from process_sales_ic_ftp_to_pg.libs.on_delivery_mapping import OnDeliveryFieldsMap


class PgSalesHook:
    def __init__(self, pg_conn_id: str):
        self.pg_conn_id = pg_conn_id

    def load(self, csv_filepath: str):
        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = pg.get_conn()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE sales.rawdata_since2024")
                with open(csv_filepath, 'r', newline='') as f:
                    cols_sql = ', '.join(SalesFieldsMap.dest_columns())
                    copy_sql = (
                        f"COPY sales.rawdata_since2024 ({cols_sql}) FROM STDIN WITH ("
                        "FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"', ENCODING 'UTF8', HEADER)"
                    )
                    cur.copy_expert(copy_sql, f)
                cur.execute(SALES_DELETE_SQL)
                cur.execute(SALES_INSERT_SQL)
            conn.commit()
        finally:
            conn.close()


class PgOnDeliveryHook:
    def __init__(self, pg_conn_id: str):
        self.pg_conn_id = pg_conn_id

    def load(self, csv_filepath: str):
        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = pg.get_conn()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE stocks.on_delivery")
                with open(csv_filepath, 'r', newline='') as f:
                    cols_sql = ', '.join(OnDeliveryFieldsMap.dest_columns())
                    copy_sql = (
                        f"COPY stocks.on_delivery ({cols_sql}) FROM STDIN WITH ("
                        "FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"', ENCODING 'UTF8', HEADER)"
                    )
                    cur.copy_expert(copy_sql, f)
            conn.commit()
        finally:
            conn.close()


