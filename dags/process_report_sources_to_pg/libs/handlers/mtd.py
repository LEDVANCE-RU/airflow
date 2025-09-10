import pandas as pd
from ..constants import KEY_TO_TABLE
from ..common import export_df, align_columns, drop_trailing_total


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('MTD_report_AG'):
        return
    df = pd.read_excel(files['MTD_report_AG'])
    period = str(df.iloc[0, 3])[8:]
    dash = period.find('-')
    mtd_from = period[0:(dash-1)]
    mtd_to = period[(dash+2):]
    period_df = pd.DataFrame({'mtd_period': [period], 'mtd_from': [pd.to_datetime(mtd_from, dayfirst=True)], 'mtd_to': [pd.to_datetime(mtd_to, dayfirst=True)]})
    period_table = 'md.mtd_report_ag_period'
    period_df = align_columns(period_df, period_table)
    table_to_file[period_table] = export_df(period_df, out_dp, 'MTD_report_AG_period', period_table)

    df = df.drop(labels=[0,1,2,3,4,5]).dropna(how='all', axis='columns')
    df = df.drop(labels=[6])
    df.columns = ['ean','ic','open_stock','in_','out_','close_stock']
    df = drop_trailing_total(df)
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    table = KEY_TO_TABLE['MTD_report_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'MTD_report_AG_data', table)
