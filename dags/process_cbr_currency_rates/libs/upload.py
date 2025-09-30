import logging
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from process_cbr_currency_rates.libs.mapping import CbrFieldsMap


class PgCbrHook(PostgresHook):
    def __init__(self, pg_conn_id: str, *args, **kwargs):
        super().__init__(pg_conn_id, *args, **kwargs)

    def _create_table(self, table_name: str, dest_map: dict):
        logging.info('Creating table %s if not exists...', table_name)
        sql_cols_str = ',\n'.join([f"{v.name} {v.type}" for v in dest_map.values()])
        self.run(f"CREATE TABLE IF NOT EXISTS {table_name} ({sql_cols_str});")
        logging.info('Table %s ensured to exist.', table_name)

    def _upsert_rates(self, table_name: str, import_filepath: str):
        tmp_table = f"{table_name}_tmp"
        dest_map = CbrFieldsMap.dest_map()
        cols = [v.name for v in dest_map.values()]
        self.run(f"DROP TABLE IF EXISTS {tmp_table};")
        self.run(f"CREATE TEMP TABLE {tmp_table} (LIKE {table_name} INCLUDING ALL);")
        copy_sql = f"""
            COPY {tmp_table} ({', '.join(cols)}) FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                NULL '',
                QUOTE '"',
                ENCODING 'UTF8',
                HEADER
            );
        """
        self.copy_expert(copy_sql, import_filepath)
        merge_sql = f"""
            INSERT INTO {table_name} ({', '.join(cols)})
            SELECT {', '.join(cols)} FROM {tmp_table}
            ON CONFLICT (currency, date)
            DO UPDATE SET rate_rub = EXCLUDED.rate_rub;
        """
        self.run(merge_sql)

    def upload_rates(self, transformed_data_json: str):
        transformed = json.loads(transformed_data_json)
        fp = transformed.get('cbr_rates')
        if not fp:
            logging.info('Nothing to upload for CBR rates.')
            return
        dest_map = CbrFieldsMap.dest_map()
        self._create_table('md.cbr_rates', dest_map)
        self.run('''
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_indexes
                    WHERE schemaname = 'md' AND indexname = 'cbr_rates_currency_date_key'
                ) THEN
                    EXECUTE 'CREATE UNIQUE INDEX cbr_rates_currency_date_key ON md.cbr_rates (currency, date)';
                END IF;
            END$$;
        ''')
        self._upsert_rates('md.cbr_rates', fp)
