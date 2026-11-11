import json

from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import task
from datetime import datetime

from constants import TZ_MSK


def _get_sftp_client():
    from airflow.providers.sftp.hooks.sftp import SFTPHook
    return SFTPHook("sftp_3pl").get_conn()

def _get_smb_hook():
    return SambaHook('smb_wms_orders_fairmark')


with DAG(
    dag_id="process_wms_orders_with_fairmarks",
    start_date=datetime(2026, 1, 1, tzinfo=TZ_MSK),
    schedule='*/30 * * * *',
    catchup=False,
    tags={'3pl', 'fairmark', 'wms', 'sftp'}
) as dag:
    @task
    def list_filenames_task() -> str:
        from process_wms_orders_with_fairmarks.libs.filter_files import filter_files
        from process_wms_orders_with_fairmarks.libs.constants import SFTP_IN_DIRPATH

        sftp_client = _get_sftp_client()
        sftp_filenames = filter_files(sftp_client, SFTP_IN_DIRPATH)
        return json.dumps(sftp_filenames)

    @task.short_circuit
    def check_sftp_files_found_task(sftp_filenames: str) -> bool:
        return len(json.loads(sftp_filenames)) > 0

    @task
    def transform_files_task(sftp_filenames: str | None):
        from process_wms_orders_with_fairmarks.libs.process_files import transform_files
        from process_wms_orders_with_fairmarks.libs.constants import SFTP_IN_DIRPATH, SMB_IN_DIRPATH

        sftp_client = _get_sftp_client()
        smb_hook = _get_smb_hook()

        transform_files(sftp_client, sftp_fnames=json.loads(sftp_filenames), sftp_dpath=SFTP_IN_DIRPATH,
                        smb_hook=smb_hook, smb_out_dpath=SMB_IN_DIRPATH)
        return True


    sftp_filenames = list_filenames_task()
    are_sftp_files_found = check_sftp_files_found_task(sftp_filenames)
    files_transformed = transform_files_task(sftp_filenames)

    are_sftp_files_found >> files_transformed
