import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE
from process_report_sources_to_pg.libs.common import export_df, align_columns, drop_trailing_total


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('1C_IC_AG'):
        return
    df = pd.read_excel(files['1C_IC_AG'], dtype='str')
    df = drop_trailing_total(df)
    df.columns = ['ic', 'project', 'hs_code', 'country_of_origin', 'localization', 'life_status', 'wh_status', 'volume']
    df = df.dropna(subset=['ic']).drop_duplicates(subset=['ic']).reset_index(drop=True)
    table = KEY_TO_TABLE['1C_IC_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, '1C_IC_AG', table)
