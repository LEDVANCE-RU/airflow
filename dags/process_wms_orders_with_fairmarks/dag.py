import json
import logging

from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import task
from datetime import datetime

from constants import TZ_MSK

from process_wms_orders_with_fairmarks.libs.constants import (
    SFTP_IN_DIRPATH,
    SFTP_OUT_DIRPATH,
    SMB_IN_DIRPATH,
    SMB_OUT_DIRPATH
)


def _get_sftp_client():
    from airflow.providers.sftp.hooks.sftp import SFTPHook
    return SFTPHook("sftp_3pl").get_conn()

def _get_smb_hook():
    return SambaHook('smb_wms_orders_fairmark')


with DAG(
    dag_id="process_wms_orders_with_fairmarks",
    start_date=datetime(2026, 1, 1, tzinfo=TZ_MSK),
    schedule='*/15 * * * *',
    catchup=False,
    tags={'3pl', 'fairmark', 'wms', 'sftp'}
) as dag:

    DIR_MAP = [
        (SFTP_IN_DIRPATH, SMB_IN_DIRPATH),
        (SFTP_OUT_DIRPATH, SMB_OUT_DIRPATH),
    ]

    @task
    def list_filenames_task(dir_map_item: tuple) -> str:
        from process_wms_orders_with_fairmarks.libs.filter_files import filter_files

        sftp_client = _get_sftp_client()
        sftp_filenames = filter_files(sftp_client, dir_map_item[0])
        return json.dumps(
            (*dir_map_item, sftp_filenames)
        )

    @task
    def transform_files_task(dir_map_item_str: str):
        from process_wms_orders_with_fairmarks.libs.process_files import transform_files

        dir_map_item = json.loads(dir_map_item_str)

        if not dir_map_item[2]:
            logging.info("No new files found in SFTP-folder '%s'. Skipping...", dir_map_item[0])
            return False

        sftp_client = _get_sftp_client()
        smb_hook = _get_smb_hook()

        transform_files(sftp_client, sftp_fnames=dir_map_item[2], sftp_dpath=dir_map_item[0],
                        smb_hook=smb_hook, smb_out_dpath=dir_map_item[1])
        return True

    @task
    def remove_old_smb_files_task(dir_map_item: str):
        from process_wms_orders_with_fairmarks.libs.process_files import remove_old_smb_files

        smb_hook = _get_smb_hook()
        remove_old_smb_files(smb_hook, dir_map_item[1])
        return True


    sftp_filenames = list_filenames_task.expand(dir_map_item=DIR_MAP)
    files_transformed = transform_files_task.expand(dir_map_item_str=sftp_filenames)
    old_files_removed = remove_old_smb_files_task.expand(dir_map_item=DIR_MAP)
