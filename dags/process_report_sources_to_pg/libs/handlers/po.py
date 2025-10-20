from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, FILE_DTYPES
from process_report_sources_to_pg.libs.common import export_df, align_columns, drop_trailing_total
import pandas as pd


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('PO_report_NEW_AG'):
        return
    df = pd.read_excel(files['PO_report_NEW_AG'], dtype=FILE_DTYPES['PO_report_NEW_AG'])
    df = drop_trailing_total(df)
    df = df.dropna(how='all', axis='columns')
    df.columns = ['project', 'life_status', 'ean', 'ic', 'description', 'date', 'po_qty']
    s = df['date'].astype(str).str.replace('/', '-', regex=False).str.strip()
    d1 = pd.to_datetime(s, errors='coerce', dayfirst=True)
    d2 = pd.to_datetime(s, format='%d-%m-%y %H:%M:%S', errors='coerce')
    d3 = pd.to_datetime(s, format='%d-%m-%y', errors='coerce')
    df['date'] = d1.fillna(d2).fillna(d3)
    df = df.dropna(subset=['date']).drop(columns=['project'])
    df['ean'] = df['ean'].str.strip()
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    df['article'] = df['ean'] + ' - ' + df['ic'].fillna('') + ' - ' + df['description']
    table = KEY_TO_TABLE['PO_report_NEW_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'PO_report_NEW_AG', table)
