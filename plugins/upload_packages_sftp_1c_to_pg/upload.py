import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

from upload_packages_sftp_1c_to_pg.mapping import PackageFieldsMap


class PgPackagesHook(PostgresHook):
    _TABLE_NAME = 'public.packages'

    def __init__(self, pg_conn_id: str, import_filepath: str, *args, **kwargs):
        super().__init__(pg_conn_id, *args, **kwargs)
        self.import_filepath = import_filepath
        self._fmap = PackageFieldsMap

    def create_table(self):
        logging.info('Creating table %s ...', self._TABLE_NAME)
        sql_cols_str = ',\n'.join([f"{v.name} {v.type}" for v in self._fmap.dest_map().values()])
        self.run(f"CREATE TABLE IF NOT EXISTS {self._TABLE_NAME} ({sql_cols_str});")
        logging.info('Table %s created.', self._TABLE_NAME)

    def clear_table(self):
        logging.info('Cleaning up table %s ...', self._TABLE_NAME)
        self.run(f"TRUNCATE TABLE {self._TABLE_NAME};")
        logging.info('Table %s has been cleaned up.', self._TABLE_NAME)

    def import_data(self):
        copy_sql = f"""
            COPY {self._TABLE_NAME} ({', '.join([c.name for c in self._fmap.dest_map().values()])}) FROM STDIN 
            WITH (
                FORMAT CSV,
                DELIMITER ',',
                NULL '',
                QUOTE '"',
                ENCODING 'UTF8',
                HEADER
            );
        """
        logging.info('Importing data from file %s to table %s ...', self.import_filepath, self._TABLE_NAME)
        self.copy_expert(copy_sql, self.import_filepath)
        logging.info('Data has been imported successfully.')