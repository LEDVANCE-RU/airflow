import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, FILE_DTYPES
from process_report_sources_to_pg.libs.common import export_df, align_columns, drop_trailing_total
import re


def read_ltm_header(file_path: str) -> pd.DataFrame:
    header_df = pd.read_excel(file_path, nrows=7)
    period_text = str(header_df.iloc[0, 3])
    dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', period_text)
    ltm_from, ltm_to = dates[0], dates[1]
    period = f"{ltm_from} - {ltm_to}"
    period_df = pd.DataFrame({
        'ltm_period': [period],
        'ltm_from': [pd.to_datetime(ltm_from, dayfirst=True)],
        'ltm_to': [pd.to_datetime(ltm_to, dayfirst=True)]
    })
    return period_df


def read_ltm_data(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, skiprows=7, dtype=FILE_DTYPES['LTM_report_AG']).dropna(how='all', axis='columns')
    df.columns = ['ean', 'ic', 'open_stock', 'in_', 'out_', 'close_stock']
    df = df.dropna(subset=['ean', 'ic'])
    df = drop_trailing_total(df)
    df['ean'] = df['ean'].str.strip()
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    return df


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('LTM_report_AG'):
        return
    
    period_df = read_ltm_header(files['LTM_report_AG'])
    period_table = 'md.ltm_report_ag_period'
    period_df = align_columns(period_df, period_table)
    table_to_file[period_table] = export_df(period_df, out_dp, 'LTM_report_AG_period', period_table)
    
    df = read_ltm_data(files['LTM_report_AG'])
    table = KEY_TO_TABLE['LTM_report_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'LTM_report_AG_data', table)
