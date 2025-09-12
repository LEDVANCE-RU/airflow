import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE
from process_report_sources_to_pg.libs.common import export_df, align_columns


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('BO_report_AG'):
        return
    df = pd.read_excel(files['BO_report_AG'])
    df.columns = ['ean', 'description', 'ic', 'uom', 'stock', 'in_outbound', 'reserved', 'available']
    df = df.drop(labels=(df.index.stop - 1), axis='index')
    df = df.drop(labels=[0]).dropna(subset=['ean']).reset_index(drop=True)
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    table = KEY_TO_TABLE['BO_report_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'BO_report_AG', table)
