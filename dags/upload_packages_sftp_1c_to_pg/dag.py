import json
import logging
import os
import uuid

from airflow import DAG
from airflow.sdk import task, teardown, Variable
from datetime import datetime

from constants import TZ_MSK
from upload_packages_sftp_1c_to_pg.libs.constants import (
    ResultKeys,
    ISSUES_NOTIFICATION_HTML,
    ISSUES_NOTIFICATION_SUBJECT,
    ISSUES_NAMES_MAP
)

with DAG(
    dag_id="upload_packages_sftp_1c_to_pg",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule='30 1 * * *',
    catchup=False,
) as dag:
    @task
    def download_task() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        local_dp = os.path.join(Variable.get('tmp_dir_path'), 'packages')
        local_fp = os.path.join(local_dp, f"{uuid.uuid4().hex}_packages.xlsx")
        remote_fp = "/packages.xlsx"

        os.makedirs(local_dp, exist_ok=True)

        sftp_hook = SFTPHook("sftp_1c")
        sftp_hook.retrieve_file(remote_fp, local_fp)
        return local_fp

    @task
    def transform_task(source_fp: str) -> str:
        from upload_packages_sftp_1c_to_pg.libs.transform import transform

        out_dp = os.path.dirname(source_fp)
        fp_map = transform(source_fp, out_dp)
        return json.dumps(fp_map)

    @task
    def upload_task(fp_map: str):
        from upload_packages_sftp_1c_to_pg.libs.upload import PgPackagesHook

        export_fp = json.loads(fp_map)[ResultKeys.EXPORT]
        pg = PgPackagesHook('pg_prod', export_fp)
        pg.create_table()
        pg.clear_table()
        pg.import_data()

    @task
    def process_issues_task(fp_map: str) -> str | None:
        from hooks.webdav import WebDAVHook

        issues_fp = {k: v for k, v in json.loads(fp_map).items() if k in ResultKeys.issue_keys() and v}
        if not issues_fp:
            return None

        webdav_hook = WebDAVHook('webdav_sharepoint_root')
        webdav_client = webdav_hook.get_conn()

        issues_fp_map = dict()
        for key, local_fp in issues_fp.items():
            remote_fp = os.path.join(Variable.get('packages_issues_sp_out_dir'), f"{key}.xlsx")
            issues_fp_map[key] = webdav_hook.get_full_path(remote_fp)
            webdav_client.upload(remote_fp, local_fp)
        return json.dumps(issues_fp_map)

    @task
    def notify_about_issues_task(issues_fp_map: str):
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        fp_map = json.loads(issues_fp_map)
        if not fp_map:
            return

        links = [f'<li><a href="{v}">{ISSUES_NAMES_MAP.get(k, k)}</a></li>' for k, v in fp_map.items()]
        html_content = ISSUES_NOTIFICATION_HTML.format(issues=''.join(links))
        addressees = json.loads(Variable.get('packages_issues_notification_addressees'))
        logging.info(addressees)

        smtp_hook = SmtpHook('smtp_sys_tech')
        smtp_hook.get_conn()
        smtp_hook.send_email_smtp(to=addressees.get('to'),
                                  cc=addressees.get('cc'),
                                  bcc=addressees.get('bcc'),
                                  subject=ISSUES_NOTIFICATION_SUBJECT,
                                  html_content=html_content)

    @teardown
    def cleanup_task(local_fp: str, interim_fp_map: str):
        filepaths = [local_fp]
        if interim_fp_map:
            filepaths.extend(json.loads(interim_fp_map).values())
        for fp in filepaths:
            if fp and os.path.exists(fp):
                os.remove(fp)
                logging.info("File %s removed.", fp)

    local_fp = download_task()
    fp_map = transform_task(local_fp)
    upload_task(fp_map)
    issues_fp_map = process_issues_task(fp_map)
    notify_about_issues_task(issues_fp_map)

    (fp_map, issues_fp_map) >> cleanup_task(local_fp, fp_map)
