import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, FILE_DTYPES
from process_report_sources_to_pg.libs.common import export_df, align_columns, drop_trailing_total, \
    extract_period_from_header


def read_mtd_header(file_path: str) -> pd.DataFrame:
    mtd_from, mtd_to = extract_period_from_header(file_path)
    period = f"{mtd_from} - {mtd_to}"
    period_df = pd.DataFrame({
        'mtd_period': [period],
        'mtd_from': [pd.to_datetime(mtd_from, dayfirst=True)],
        'mtd_to': [pd.to_datetime(mtd_to, dayfirst=True)]
    })
    return period_df



def read_mtd_data(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, skiprows=7, dtype=FILE_DTYPES['MTD_report_AG']).dropna(how='all', axis='columns')
    df.columns = ['ean', 'ic', 'open_stock', 'in_', 'out_', 'close_stock']
    df = drop_trailing_total(df)
    df['ean'] = df['ean'].str.strip()
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    return df


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('MTD_report_AG'):
        return
    
    period_df = read_mtd_header(files['MTD_report_AG'])
    period_table = 'md.mtd_report_ag_period'
    period_df = align_columns(period_df, period_table)
    table_to_file[period_table] = export_df(period_df, out_dp, 'MTD_report_AG_period', period_table)
    
    df = read_mtd_data(files['MTD_report_AG'])
    table = KEY_TO_TABLE['MTD_report_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'MTD_report_AG_data', table)
