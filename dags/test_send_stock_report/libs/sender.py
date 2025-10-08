import logging
import os
import re
import pandas as pd
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from test_send_stock_report.libs.constants import STOCK_REPORT_SQL

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

    filename = f'АО ЛЕДВАНС остатки на {datetime.now().strftime("%d.%m.%Y")}.xlsx'
    filepath = os.path.join(tmp_dir, filename)

    report_df.to_excel(filepath, index=False)
    logging.info("Report saved to %s", filepath)

    to = recipients.get('to') or []
    cc = recipients.get('cc') or []
    bcc = recipients.get('bcc') or []

    msg = MIMEMultipart()
    msg['Subject'] = 'АО "ЛЕДВАНС": Остатки на складе на текущую дату'
    msg['From'] = 'smtp_sys_tech'
    msg['To'] = ', '.join(to) if isinstance(to, list) else to
    if cc:
        msg['Cc'] = ', '.join(cc) if isinstance(cc, list) else cc

    html_body = (
        "<p>Добрый день,</p>" 
        "<p>Письмо было сформировано и отправлено автоматически. Просьба не отвечать на данное письмо.</p>"
        "<p>По всем возникающим вопросам Вы можете обращаться к ответственному сотруднику отдела по работе с клиентами АО «ЛЕДВАНС»</p>"
    )
    msg.attach(MIMEText(html_body, 'html'))

    with open(filepath, 'rb') as f:
        attachment = MIMEApplication(f.read(), _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(attachment)

    smtp_hook = SmtpHook('smtp_sys_tech')
    smtp_conn = smtp_hook.get_conn()
    
    all_recipients = to + cc + bcc
    smtp_conn.sendmail(msg['From'], all_recipients, msg.as_string())
    smtp_conn.quit()
    
    logging.info("Email sent to: to=%s cc=%s bcc=%s", to, cc, bcc)
