from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from lemana_sales_report.libs.transform import DB_COLUMNS

TABLE = "retail.lemana_sales"


class PgLemanaSalesHook:
    def __init__(self, pg_conn_id: str):
        self.pg_conn_id = pg_conn_id

    def replace_months(self, df) -> int:
        """Заменяет в БД данные за месяцы, присутствующие в df, на данные
        из df. Удаление и вставка выполняются в одной транзакции: если
        вставка упадёт, удалённые строки вернутся обратно (rollback).

        Делает загрузку идемпотентной (безопасно перезапускать) - при
        каждом запуске данные в БД просто приводятся в соответствие с
        последним состоянием отчёта."""
        months = sorted(df["month"].dropna().unique().tolist())
        if not months:
            return 0

        data = df[DB_COLUMNS].values.tolist()
        insert_sql = f"INSERT INTO {TABLE} ({', '.join(DB_COLUMNS)}) VALUES %s"

        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = pg.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {TABLE} WHERE month = ANY(%s)", (months,))
                execute_values(cur, insert_sql, data, page_size=5000)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return len(data)
