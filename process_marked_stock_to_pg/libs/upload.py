import logging
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from process_marked_stock_to_pg.libs.mapping import SiFieldsMap

class PgSiHook(PostgresHook):
    def __init__(self, pg_conn_id: str, *args, **kwargs):
        super().__init__(pg_conn_id, *args, **kwargs)

    def _execute_upload(self, table_name: str, dest_map: dict, import_filepath: str):
        if not dest_map:
            logging.warning(f"Destination map for table {table_name} is empty. Skipping upload.")
            return

        self._create_table(table_name, dest_map)
        self._clear_table(table_name)
        self._import_data(table_name, dest_map, import_filepath)

    def _create_table(self, table_name: str, dest_map: dict):
        logging.info(f'Creating table {table_name} if not exists...')
        sql_cols_str = ',\n'.join([f'"{v.name}" {v.type}' for v in dest_map.values()])
        self.run(f'CREATE SCHEMA IF NOT EXISTS si;')
        self.run(f"CREATE TABLE IF NOT EXISTS {table_name} ({sql_cols_str});")
        logging.info(f'Table {table_name} ensured to exist.')

    def _clear_table(self, table_name: str):
        logging.info(f'Cleaning up table {table_name} ...')
        self.run(f"TRUNCATE TABLE {table_name};")
        logging.info(f'Table {table_name} has been cleaned up.')

    def _import_data(self, table_name: str, dest_map: dict, import_filepath: str):
        columns = [f'"{v.name}"' for v in dest_map.values()]
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
        logging.info(f'Importing data from file {import_filepath} to table {table_name} ...')
        self.copy_expert(copy_sql, import_filepath)
        logging.info('Data has been imported successfully.')

    def upload_data(self, transformed_files_json: str):
        transformed_files = json.loads(transformed_files_json)
        
        upload_params = {
            "marked_stock_ic": {"dest_map": SiFieldsMap.marked_stock_dest_map, "table_name": "si.marked_stock_ic"},
        }
        
        for key, params in upload_params.items():
            fp = transformed_files.get(key)
            if fp:
                self._execute_upload(
                    table_name=params["table_name"],
                    dest_map=params["dest_map"](),
                    import_filepath=fp
                )
