import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.sdk import task, Variable
from airflow.providers.ssh.hooks.ssh import SSHHook

from constants import TZ_MSK
from plugins.hooks.webdav import WebDAVHook


DAG_ID = "copy_invoices_ftp_to_sharepoint"
SCHEDULE = '30 8 * * 1-5'  # 08:30 MSK Mon-Fri

# Source FTP connection and base path
FTP_CONN_ID = Variable.get('invoices_ftp_conn_id', default='sftp_1c')
FTP_BASE_PATH = Variable.get('invoices_ftp_base_path', default='/path/to/invoices')

# Destination SharePoint WebDAV connection and relative target folder
WEBDAV_CONN_ID = Variable.get('invoices_webdav_conn_id', default='webdav_sharepoint_root')
WEBDAV_TARGET_RELATIVE_PATH = Variable.get(
    'invoices_webdav_target_rel_path',
    default='MSK-files/TRADE/Invoices'
)

FILENAMES = [
    'actual sum and cur (invoice).txt',
    'Invoice (actual payment date).txt',
    'Invoice (transit).txt',
    'Invoice PO.txt',
    'Plan (invoice).txt',
    'UNK_Invoice_new_version_2.txt',
    'Плановое погашение_16.08.2024.txt',
]


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule=SCHEDULE,
    catchup=False,
    tags=['invoices', 'ftp', 'sharepoint', 'webdav'],
) as dag:

    @task
    def copy_files_task():
        # Prepare hooks
        ssh_hook = SSHHook(ssh_conn_id=FTP_CONN_ID)
        webdav_hook = WebDAVHook(conn_id=WEBDAV_CONN_ID)

        # Ensure target folder exists (best-effort)
        client = webdav_hook.get_conn()
        target_folder = WEBDAV_TARGET_RELATIVE_PATH.strip('/')
        try:
            if not client.check(remote_path=target_folder):
                client.mkdir(remote_path=target_folder)
        except Exception as exc:
            logging.warning("Cannot ensure target folder exists at '%s': %s", target_folder, exc)

        # Copy each file
        for filename in FILENAMES:
            source_path = os.path.join(FTP_BASE_PATH, filename)
            target_path = f"{target_folder}/{filename}"

            try:
                # Download from FTP/SFTP to a temporary local path via SSH
                with ssh_hook.get_conn() as ssh_client:
                    sftp = ssh_client.open_sftp()
                    try:
                        with sftp.file(source_path, mode='rb') as remote_file:
                            file_bytes = remote_file.read()
                    finally:
                        sftp.close()

                # Upload to SharePoint via WebDAV
                client.resource(target_path).write(file_bytes)
                logging.info("Copied '%s' -> '%s'", source_path, target_path)
            except FileNotFoundError:
                logging.warning("Source file not found on FTP: %s. Skipping.", source_path)
            except Exception as exc:
                logging.error("Failed to copy '%s' to '%s': %s", source_path, target_path, exc)

    copy_files_task()


