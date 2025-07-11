import logging
import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

def get_stock_report_df(pg_conn_id: str) -> pd.DataFrame:
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    logging.info("Fetching stock report from si.stock_report()...")

    sql_query = "SELECT * FROM si.stock_report()"
    report_df = pd.read_sql_query(sql_query, conn)
    logging.info("Successfully fetched %s rows.", len(report_df))
    
    return report_df

def send_report_by_email(report_df: pd.DataFrame, recipients: list[str], tmp_dir: str):
    if report_df.empty:
        logging.info("Report is empty, skipping email.")
        return

    filename = 'stock_report.xlsx'
    filepath = os.path.join(tmp_dir, filename)

    report_df.to_excel(filepath, index=False)
    logging.info("Report saved to %s", filepath)

    send_email(
        to=recipients,
        subject='Stock Report',
        html_content='Please find the attached stock report.',
        files=[filepath],
    )
    logging.info("Email sent to: %s", recipients)
