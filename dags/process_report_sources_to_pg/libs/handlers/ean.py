import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, FILE_DTYPES
from process_report_sources_to_pg.libs.common import export_df, align_columns


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('1C_EAN_AG'):
        return
    df = pd.read_excel(files['1C_EAN_AG'], usecols=[0, 1, 3], dtype=FILE_DTYPES['1C_EAN_AG'])
    df.columns = ['description', 'ean', 'hs_code']
    df['ean'] = df['ean'].str.strip()
    df = df.dropna(subset=['ean']).drop_duplicates(subset=['ean']).reset_index(drop=True)
    table = KEY_TO_TABLE['1C_EAN_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, '1C_EAN_AG', table)
