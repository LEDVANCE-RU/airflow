import logging
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from process_report_sources_to_pg.libs.constants import SQL_CREATE_TABLE, TABLE_COLUMNS


class PgReportSourcesHook(PostgresHook):
    def __init__(self, pg_conn_id: str, *args, **kwargs):
        super().__init__(pg_conn_id, *args, **kwargs)

    def _create_table(self, table_name: str):
        sql = SQL_CREATE_TABLE.get(table_name)
        if not sql:
            return
        logging.info('Ensuring table %s exists...', table_name)
        self.run(sql)

    def _clear_table(self, table_name: str):
        logging.info('Truncating table %s ...', table_name)
        self.run(f"TRUNCATE TABLE {table_name};")

    def _import_data(self, table_name: str, columns: list[str], fp: str):
        copy_sql = f"""
            COPY {table_name} ({', '.join(columns)}) FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                NULL '',
                QUOTE '"',
                ENCODING 'UTF8',
                HEADER
            );
        """
        logging.info('Importing data from %s to %s ...', fp, table_name)
        self.copy_expert(copy_sql, fp)

    def upload_data(self, table_to_file_json: str):
        table_to_file = json.loads(table_to_file_json)
        for table_name, fp in table_to_file.items():
            self._create_table(table_name)
            self._clear_table(table_name)
            columns = TABLE_COLUMNS.get(table_name)
            self._import_data(table_name, columns, fp)


