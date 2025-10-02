import pandas as pd
from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, FILE_DTYPES
from process_report_sources_to_pg.libs.common import export_df, align_columns, drop_trailing_total


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('1C_master_data_AG'):
        return
    df = pd.read_excel(files['1C_master_data_AG'], dtype=FILE_DTYPES['1C_master_data_AG'])
    df = drop_trailing_total(df)
    df.columns = ['description', 'uom', 'article', 'import_', 'kind_of_goods', 'type_of_goods', 'price_group', 'analytic_group', 'fin_group', 'hs_code', 'co_o', 'nom_group', 'nom_group_group', 'nom_group_group_group', 'nom_group_group_group_group', 'volume']
    df['article'] = df['article'].str.strip().str.extract(r'(\d+)', expand=False)
    df = df.dropna(subset=['article']).reset_index(drop=True)
    table = KEY_TO_TABLE['1C_master_data_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, '1C_master_data_AG', table)
