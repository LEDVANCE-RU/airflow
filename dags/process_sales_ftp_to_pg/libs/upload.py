import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from process_sales_ftp_to_pg.libs.constants import SALES_DELETE_SQL, SALES_INSERT_SQL
from process_sales_ftp_to_pg.libs.mapping import SalesFieldsMap


class PgSalesHook:
    def __init__(self, pg_conn_id: str):
        self.pg_conn_id = pg_conn_id

    def truncate_and_copy(self, csv_filepath: str):
        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = pg.get_conn()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE sales.sales_raw")
            with open(csv_filepath, 'r', newline='') as f:
                cols = SalesFieldsMap.dest_columns()
                cols_sql = ', '.join(cols)
                copy_sql = (
                    f"COPY sales.sales_raw ({cols_sql}) FROM STDIN WITH ("
                    "FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"', ENCODING 'UTF8', HEADER)"
                )
                cur.copy_expert(copy_sql, f)
        conn.commit()

    def call_raw_to_ns(self):
        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = pg.get_conn()
        with conn.cursor() as cur:
            cur.execute(SALES_DELETE_SQL)
            cur.execute(SALES_INSERT_SQL)
        conn.commit()


