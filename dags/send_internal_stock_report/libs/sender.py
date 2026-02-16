import logging
import os
import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.smtp import SmtpExtHook, Attachment, MimeAppTypeMap
from send_internal_stock_report.libs.constants import INSERT_STOCK_REPORT_SQL, SELECT_STOCK_REPORT_SQL


def get_stock_report_df(pg_conn_id: str) -> pd.DataFrame:
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()

    logging.info("Inserting stock report data...")
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(INSERT_STOCK_REPORT_SQL)
    conn.commit()

    logging.info("Reading stock report data...")
    report_df = pd.read_sql(SELECT_STOCK_REPORT_SQL, pg_hook.get_uri())
    logging.info("Successfully fetched %s rows.", len(report_df))
    
    return report_df


def send_report_by_email(report_df: pd.DataFrame, recipients: dict, tmp_dir: str):
    if report_df.empty:
        logging.info("Report is empty, skipping email.")
        return

    filename = f'Stock_Inbounds {datetime.now().strftime("%d.%m.%Y")}.xlsx'
    filepath = os.path.join(tmp_dir, filename)

    report_df.to_excel(filepath, index=False)
    logging.info("Report saved to %s", filepath)

    to = recipients.get('to') or []
    cc = recipients.get('cc') or []
    bcc = recipients.get('bcc') or []

    smtp_hook = SmtpExtHook('smtp_sys_tech')
    smtp_hook.send_email_smtp(
        to=to,
        cc=cc,
        bcc=bcc,
        subject='Stock_Inbounds',
        html_content=(
            "<p>Добрый день,</p>"
            "<p>Письмо было сформировано и отправлено автоматически. Просьба не отвечать на данное письмо.</p>"
            "<p>По всем возникающим вопросам Вы можете обращаться на почту bi@ledvance.ru</p>"
        ),
        files=[Attachment(filepath, mime_type=MimeAppTypeMap.EXCEL)],
    )
    logging.info("Email sent to: to=%s cc=%s bcc=%s", to, cc, bcc)
    return filepath
