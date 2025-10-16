import logging
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from process_project_base_1c_to_pg.libs.mapping import ProjectBaseFieldsMap


class PgProjectBaseHook(PostgresHook):
    def __init__(self, pg_conn_id: str, *args, **kwargs):
        super().__init__(pg_conn_id, *args, **kwargs)

    def _execute_upload(self, table_name: str, dest_map: dict, import_filepath: str):
        self._create_table(table_name, dest_map)
        self._clear_table(table_name)
        self._import_data(table_name, dest_map, import_filepath)

    def _create_table(self, table_name: str, dest_map: dict):
        logging.info('Creating table %s if not exists...', table_name)
        sql_cols_str = ',\n'.join([f"{v.name} {v.type}" for v in dest_map.values()])
        self.run(f"CREATE TABLE IF NOT EXISTS {table_name} ({sql_cols_str});")
        logging.info('Table %s ensured to exist.', table_name)

    def _clear_table(self, table_name: str):
        logging.info('Cleaning up table %s ...', table_name)
        self.run(f"TRUNCATE TABLE {table_name};")
        logging.info('Table %s has been cleaned up.', table_name)

    def _import_data(self, table_name: str, dest_map: dict, import_filepath: str):
        columns = [v.name for v in dest_map.values()]
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
        logging.info('Importing data from file %s to table %s ...', import_filepath, table_name)
        self.copy_expert(copy_sql, import_filepath)
        logging.info('Data has been imported successfully.')

    def upload_data(self, transformed_files_json: str):
        transformed_files = json.loads(transformed_files_json)

        fp = transformed_files.get("project_base")
        if fp:
            self._execute_upload(
                table_name='md.project_base',
                dest_map=ProjectBaseFieldsMap.dest_map(),
                import_filepath=fp
            )


