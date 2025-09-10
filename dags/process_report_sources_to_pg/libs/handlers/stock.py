from ..constants import KEY_TO_TABLE
from ..common import export_df, align_columns, read_drop_last_row


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('STOCK_report_AG'):
        return
    df = read_drop_last_row(files['STOCK_report_AG'])
    df.columns = ['ean', 'description', 'ic', 'uom', 'open_stock_pce', 'open_stock_rub', 'stock_pce', 'stock_rub']
    df = df.dropna(subset=['ean'], how='any', ignore_index=True)
    df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
    df['variance_rub'] = df['stock_rub'].fillna(0) - df['open_stock_rub'].fillna(0)
    table = KEY_TO_TABLE['STOCK_report_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, 'STOCK_report_AG', table)
