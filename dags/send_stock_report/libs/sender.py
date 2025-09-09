import logging
import os
import re
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from send_stock_report.libs.constants import STOCK_REPORT_SQL

def get_stock_report_df(pg_conn_id: str) -> pd.DataFrame:
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    logging.info("Fetching stock report via SQL...")

    report_df = pd.read_sql_query(STOCK_REPORT_SQL, conn)
    logging.info("Successfully fetched %s rows.", len(report_df))
    
    return report_df

def send_report_by_email(report_df: pd.DataFrame, recipients: dict, tmp_dir: str):
    if report_df.empty:
        logging.info("Report is empty, skipping email.")
        return

    filename = 'stock_report.xlsx'
    filepath = os.path.join(tmp_dir, filename)

    report_df.to_excel(filepath, index=False)
    logging.info("Report saved to %s", filepath)

    to = recipients.get('to') or []
    cc = recipients.get('cc') or []
    bcc = recipients.get('bcc') or []

    smtp_hook = SmtpHook('smtp_sys_tech')
    smtp_hook.get_conn()
    smtp_hook.send_email_smtp(
        to=to,
        cc=cc,
        bcc=bcc,
        subject='АО "ЛЕДВАНС": Остатки на складе на текущую дату',
        html_content=(
            "<p>Добрый день,</p>" 
            "<p>Письмо было сформировано и отправлено автоматически. Просьба не отвечать на данное письмо.</p>"
            "<p>По всем возникающим вопросам Вы можете обращаться к ответственному сотруднику отдела по работе с клиентами АО «ЛЕДВАНС»</p>"
            ),
        files=[filepath],
    )
    logging.info("Email sent to: to=%s cc=%s bcc=%s", to, cc, bcc)
