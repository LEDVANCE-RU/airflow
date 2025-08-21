import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
                cur.copy_expert("COPY sales.sales_raw FROM STDIN WITH CSV HEADER", f)
        conn.commit()

    def call_raw_to_ns(self):
        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = pg.get_conn()
        with conn.cursor() as cur:
            cur.execute("CALL sales.sales_raw_to_ns()")
        conn.commit()


