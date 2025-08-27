import logging
import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook

def get_stock_report_df(pg_conn_id: str) -> pd.DataFrame:
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    logging.info("Fetching stock report via SQL...")

    sql_query = """
    WITH stocks AS (
        SELECT
          ean,
          SUM(COALESCE(avail, 0)) AS avail
        FROM
          si.stock_for_customer
        GROUP BY ean
      ),
      pl AS (
        SELECT DISTINCT
          ean,
          description,
          9 AS id
        FROM md.price_list
        WHERE description != 'NaN'
        UNION
        SELECT
          ean::varchar,
          description,
          1000000
        FROM
          si.ean_add
      )
      SELECT
        p.ean::numeric,
        p.description,
        ROUND(COALESCE(s.avail, 0))::numeric
      FROM
        pl p
        LEFT JOIN stocks s ON (p.ean = s.ean)
      ORDER BY p.id;
    """
    report_df = pd.read_sql_query(sql_query, conn)
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
    smtp_hook.send_email_smtp(
        to=to,
        cc=cc,
        bcc=bcc,
        subject='Stock Report',
        html_content='Please find the attached stock report.',
        files=[filepath],
    )
    logging.info("Email sent to: to=%s cc=%s bcc=%s", to, cc, bcc)
