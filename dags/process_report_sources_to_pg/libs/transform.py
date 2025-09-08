import csv
import json
import os
import uuid
import pandas as pd
import logging

from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, PERIOD_TABLES


def _export_df(df: pd.DataFrame, out_dp: str, key: str) -> str:
    export_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}_{key}.csv")
    df.to_csv(export_fp,
              index=False,
              encoding='utf-8',
              sep=',',
              quotechar='"',
              quoting=csv.QUOTE_MINIMAL)
    logging.info("Exported %s rows to %s", len(df), export_fp)
    return export_fp


def _read_drop_last_row(fp: str) -> pd.DataFrame:
    df = pd.read_excel(fp)
    if len(df) > 0:
        df = df.drop(labels=(df.index.stop - 1), axis='index')
    return df


def _drop_trailing_total(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0:
        return df
    last = df.tail(1)
    has_total = last.astype(str).apply(lambda s: s.str.contains('total', case=False, na=False)).any(axis=None)
    if has_total:
        return df.iloc[:-1]
    return df


def transform_report_sources(downloaded_files_json: str, out_dp: str) -> str:
    os.makedirs(out_dp, exist_ok=True)
    files = json.loads(downloaded_files_json)

    table_to_file: dict[str, str] = {}

    if files.get('1C_master_data_AG'):
        df = _read_drop_last_row(files['1C_master_data_AG'])
        df.columns = ['description', 'uom', 'article', 'import_', 'kind_of_goods', 'type_of_goods', 'price_group', 'analytic_group', 'fin_group', 'hs_code', 'co_o', 'nom_group', 'nom_group_group', 'nom_group_group_group', 'nom_group_group_group_group', 'volume']
        df['article'] = df['article'].astype(str).str.extract(r'(\d+)', expand=False)
        df = df.dropna(subset=['article']).reset_index(drop=True)
        table_to_file[KEY_TO_TABLE['1C_master_data_AG']] = _export_df(df, out_dp, '1C_master_data_AG')

    if files.get('MTD_report_AG'):
        df = pd.read_excel(files['MTD_report_AG'])
        period = str(df.iloc[0, 3])[8:]
        dash = period.find('-')
        mtd_from = period[0:(dash-1)]
        mtd_to = period[(dash+2):]
        period_df = pd.DataFrame({'mtd_period': [period], 'mtd_from': [pd.to_datetime(mtd_from, dayfirst=True)], 'mtd_to': [pd.to_datetime(mtd_to, dayfirst=True)]})
        table_to_file[PERIOD_TABLES['MTD_report_AG']] = _export_df(period_df, out_dp, 'MTD_report_AG_period')

        df = df.drop(labels=[0,1,2,3,4,5]).dropna(how='all', axis='columns')
        df = df.drop(labels=[6])
        df.columns = ['ean','ic','open_stock','in_','out_','close_stock']
        df = _drop_trailing_total(df)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        table_to_file[KEY_TO_TABLE['MTD_report_AG']] = _export_df(df, out_dp, 'MTD_report_AG_data')

    if files.get('LTM_report_AG'):
        df = pd.read_excel(files['LTM_report_AG'])
        period = str(df.iloc[0, 3])[8:]
        dash = period.find('-')
        ltm_from = period[0:(dash-1)]
        ltm_to = period[(dash+2):]
        period_df = pd.DataFrame({'ltm_period': [period], 'ltm_from': [pd.to_datetime(ltm_from, dayfirst=True)], 'ltm_to': [pd.to_datetime(ltm_to, dayfirst=True)]})
        table_to_file[PERIOD_TABLES['LTM_report_AG']] = _export_df(period_df, out_dp, 'LTM_report_AG_period')

        df = df.drop(labels=[0,1,2,3,4,5]).dropna(how='all', axis='columns')
        df = df.drop(labels=[6])
        df.columns = ['ean','ic','open_stock','in_','out_','close_stock']
        df = df.dropna(subset=['ean','ic'])
        df = _drop_trailing_total(df)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        table_to_file[KEY_TO_TABLE['LTM_report_AG']] = _export_df(df, out_dp, 'LTM_report_AG_data')

    if files.get('STOCK_report_AG'):
        df = _read_drop_last_row(files['STOCK_report_AG'])
        df.columns = ['ean', 'description', 'ic', 'uom', 'open_stock_pce', 'open_stock_rub', 'stock_pce', 'stock_rub']
        df = df.dropna(subset=['ean'], how='any', ignore_index=True)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        df['variance_rub'] = df['stock_rub'].fillna(0) - df['open_stock_rub'].fillna(0)
        table_to_file[KEY_TO_TABLE['STOCK_report_AG']] = _export_df(df, out_dp, 'STOCK_report_AG')

    if files.get('1C_IC_AG'):
        df = pd.read_excel(files['1C_IC_AG'], dtype='str')
        df = _drop_trailing_total(df)
        df.columns = ['ic', 'project', 'hs_code', 'country_of_origin', 'localization', 'life_status', 'wh_status', 'volume']
        df = df.dropna(subset=['ic']).drop_duplicates(subset=['ic']).reset_index(drop=True)
        table_to_file[KEY_TO_TABLE['1C_IC_AG']] = _export_df(df, out_dp, '1C_IC_AG')

    if files.get('1C_EAN_AG'):
        df = pd.read_excel(files['1C_EAN_AG'], dtype='str')
        df.columns = ['description', 'ean', 'descr_for_printing', 'hs_code']
        df = df.drop(columns=['descr_for_printing'])
        df = df.dropna(subset=['ean']).drop_duplicates(subset=['ean']).reset_index(drop=True)
        table_to_file[KEY_TO_TABLE['1C_EAN_AG']] = _export_df(df, out_dp, '1C_EAN_AG')

    if files.get('PO_report_NEW_AG'):
        df = _read_drop_last_row(files['PO_report_NEW_AG'])
        df = df.dropna(how='all', axis='columns')
        df.columns = ['project', 'life_status', 'ean', 'ic', 'description', 'date', 'po_qty']
        df = df.dropna(subset=['date']).drop(columns=['project'])
        df['date'] = pd.to_datetime(df['date'].astype(str).str.replace('/', '.'), dayfirst=True)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        df['article'] = df['ean'] + ' - ' + df['ic'].fillna('') + ' - ' + df['description']
        table_to_file[KEY_TO_TABLE['PO_report_NEW_AG']] = _export_df(df, out_dp, 'PO_report_NEW_AG')

    if files.get('BO_report_AG'):
        df = pd.read_excel(files['BO_report_AG'])
        df.columns = ['ean', 'description', 'ic', 'uom', 'stock', 'in_outbound', 'reserved', 'available']
        df = df.drop(labels=(df.index.stop - 1), axis='index')
        df = df.drop(labels=[0]).dropna(subset=['ean']).reset_index(drop=True)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        table_to_file[KEY_TO_TABLE['BO_report_AG']] = _export_df(df, out_dp, 'BO_report_AG')

    return json.dumps(table_to_file)


