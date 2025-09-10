import pandas as pd
from ..constants import KEY_TO_TABLE
from ..common import export_df, align_columns


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('1C_EAN_AG'):
        return
    df = pd.read_excel(files['1C_EAN_AG'], dtype='str')
    df.columns = ['description', 'ean', 'descr_for_printing', 'hs_code']
    df = df.drop(columns=['descr_for_printing'])
    df = df.dropna(subset=['ean']).drop_duplicates(subset=['ean']).reset_index(drop=True)
    table = KEY_TO_TABLE['1C_EAN_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, '1C_EAN_AG', table)
