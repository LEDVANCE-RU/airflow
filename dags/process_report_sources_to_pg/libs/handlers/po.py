from ..constants import KEY_TO_TABLE
from ..common import export_df, align_columns, read_drop_last_row
import pandas as pd


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('PO_report_NEW_AG'):
        return
    df = read_drop_last_row(files['PO_report_NEW_AG'])
    df = df.dropna(how='all', axis='columns')
    df.columns = ['project', 'life_status', 'ean', 'ic', 'description', 'date', 'po_qty']
    df = df.dropna(subset=['date']).drop(columns=['project'])
    df['date'] = pd.to_datetime(df['date'].astype(str).str.replace('/', '.'), dayfirst=True)
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    df['article'] = df['ean'] + ' - ' + df['ic'].fillna('') + ' - ' + df['description']
    table = KEY_TO_TABLE['PO_report_NEW_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'PO_report_NEW_AG', table)
