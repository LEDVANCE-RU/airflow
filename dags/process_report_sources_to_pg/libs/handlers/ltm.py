import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE
from process_report_sources_to_pg.libs.common import export_df, align_columns, drop_trailing_total
import re


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('LTM_report_AG'):
        return
    header_df = pd.read_excel(files['LTM_report_AG'], nrows=7)
    period_text = str(header_df.iloc[0, 3])
    dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', period_text)
    ltm_from, ltm_to = dates[0], dates[1]
    period = f"{ltm_from} - {ltm_to}"
    period_df = pd.DataFrame({'ltm_period': [period], 'ltm_from': [pd.to_datetime(ltm_from, dayfirst=True)], 'ltm_to': [pd.to_datetime(ltm_to, dayfirst=True)]})
    period_table = 'md.ltm_report_ag_period'
    period_df = align_columns(period_df, period_table)
    table_to_file[period_table] = export_df(period_df, out_dp, 'LTM_report_AG_period', period_table)

    df = pd.read_excel(files['LTM_report_AG'], skiprows=7).dropna(how='all', axis='columns')
    df.columns = ['ean','ic','open_stock','in_','out_','close_stock']
    df = df.dropna(subset=['ean','ic'])
    df = drop_trailing_total(df)
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    table = KEY_TO_TABLE['LTM_report_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'LTM_report_AG_data', table)
