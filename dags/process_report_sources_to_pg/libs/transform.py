import csv
import json
import os
import uuid
import pandas as pd
import logging

from process_report_sources_to_pg.libs.constants import KEY_TO_TABLE, PERIOD_TABLES, TABLE_COLUMNS


def _export_df(df: pd.DataFrame, out_dp: str, key: str, columns: list[str] | None = None) -> str:
    export_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}_{key}.csv")
    df.to_csv(export_fp,
              index=False,
              encoding='utf-8',
              sep=',',
              quotechar='"',
              quoting=csv.QUOTE_MINIMAL,
              columns=columns)
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


def _align_columns(df: pd.DataFrame, dest_columns: list[str]) -> pd.DataFrame:
    if not dest_columns:
        return df
    present = [c for c in df.columns if c in dest_columns]
    df = df[present]
    for col in dest_columns:
        if col not in df.columns:
            df[col] = None
    return df[dest_columns]


def transform_report_sources(downloaded_files_json: str, out_dp: str) -> str:
    os.makedirs(out_dp, exist_ok=True)
    files = json.loads(downloaded_files_json)

    table_to_file: dict[str, str] = {}

    if files.get('1C_master_data_AG'):
        df = _read_drop_last_row(files['1C_master_data_AG'])
        df.columns = ['description', 'uom', 'article', 'import_', 'kind_of_goods', 'type_of_goods', 'price_group', 'analytic_group', 'fin_group', 'hs_code', 'co_o', 'nom_group', 'nom_group_group', 'nom_group_group_group', 'nom_group_group_group_group', 'volume']
        df['article'] = df['article'].astype(str).str.extract(r'(\d+)', expand=False)
        df = df.dropna(subset=['article']).reset_index(drop=True)
        table = KEY_TO_TABLE['1C_master_data_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, '1C_master_data_AG', columns=TABLE_COLUMNS.get(table))

    if files.get('MTD_report_AG'):
        df = pd.read_excel(files['MTD_report_AG'])
        period = str(df.iloc[0, 3])[8:]
        dash = period.find('-')
        mtd_from = period[0:(dash-1)]
        mtd_to = period[(dash+2):]
        period_df = pd.DataFrame({'mtd_period': [period], 'mtd_from': [pd.to_datetime(mtd_from, dayfirst=True)], 'mtd_to': [pd.to_datetime(mtd_to, dayfirst=True)]})
        period_table = PERIOD_TABLES['MTD_report_AG']
        period_df = _align_columns(period_df, TABLE_COLUMNS.get(period_table, []))
        table_to_file[period_table] = _export_df(period_df, out_dp, 'MTD_report_AG_period', columns=TABLE_COLUMNS.get(period_table))

        df = df.drop(labels=[0,1,2,3,4,5]).dropna(how='all', axis='columns')
        df = df.drop(labels=[6])
        df.columns = ['ean','ic','open_stock','in_','out_','close_stock']
        df = _drop_trailing_total(df)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        table = KEY_TO_TABLE['MTD_report_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, 'MTD_report_AG_data', columns=TABLE_COLUMNS.get(table))

    if files.get('LTM_report_AG'):
        df = pd.read_excel(files['LTM_report_AG'])
        period = str(df.iloc[0, 3])[8:]
        dash = period.find('-')
        ltm_from = period[0:(dash-1)]
        ltm_to = period[(dash+2):]
        period_df = pd.DataFrame({'ltm_period': [period], 'ltm_from': [pd.to_datetime(ltm_from, dayfirst=True)], 'ltm_to': [pd.to_datetime(ltm_to, dayfirst=True)]})
        period_table = PERIOD_TABLES['LTM_report_AG']
        period_df = _align_columns(period_df, TABLE_COLUMNS.get(period_table, []))
        table_to_file[period_table] = _export_df(period_df, out_dp, 'LTM_report_AG_period', columns=TABLE_COLUMNS.get(period_table))

        df = df.drop(labels=[0,1,2,3,4,5]).dropna(how='all', axis='columns')
        df = df.drop(labels=[6])
        df.columns = ['ean','ic','open_stock','in_','out_','close_stock']
        df = df.dropna(subset=['ean','ic'])
        df = _drop_trailing_total(df)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        table = KEY_TO_TABLE['LTM_report_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, 'LTM_report_AG_data', columns=TABLE_COLUMNS.get(table))

    if files.get('STOCK_report_AG'):
        df = _read_drop_last_row(files['STOCK_report_AG'])
        df.columns = ['ean', 'description', 'ic', 'uom', 'open_stock_pce', 'open_stock_rub', 'stock_pce', 'stock_rub']
        df = df.dropna(subset=['ean'], how='any', ignore_index=True)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        df['variance_rub'] = df['stock_rub'].fillna(0) - df['open_stock_rub'].fillna(0)
        table = KEY_TO_TABLE['STOCK_report_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, 'STOCK_report_AG', columns=TABLE_COLUMNS.get(table))

    if files.get('1C_IC_AG'):
        df = pd.read_excel(files['1C_IC_AG'], dtype='str')
        df = _drop_trailing_total(df)
        df.columns = ['ic', 'project', 'hs_code', 'country_of_origin', 'localization', 'life_status', 'wh_status', 'volume']
        df = df.dropna(subset=['ic']).drop_duplicates(subset=['ic']).reset_index(drop=True)
        table = KEY_TO_TABLE['1C_IC_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, '1C_IC_AG', columns=TABLE_COLUMNS.get(table))

    if files.get('1C_EAN_AG'):
        df = pd.read_excel(files['1C_EAN_AG'], dtype='str')
        df.columns = ['description', 'ean', 'descr_for_printing', 'hs_code']
        df = df.drop(columns=['descr_for_printing'])
        df = df.dropna(subset=['ean']).drop_duplicates(subset=['ean']).reset_index(drop=True)
        table = KEY_TO_TABLE['1C_EAN_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, '1C_EAN_AG', columns=TABLE_COLUMNS.get(table))

    if files.get('PO_report_NEW_AG'):
        df = _read_drop_last_row(files['PO_report_NEW_AG'])
        df = df.dropna(how='all', axis='columns')
        df.columns = ['project', 'life_status', 'ean', 'ic', 'description', 'date', 'po_qty']
        df = df.dropna(subset=['date']).drop(columns=['project'])
        df['date'] = pd.to_datetime(df['date'].astype(str).str.replace('/', '.'), dayfirst=True)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        df['article'] = df['ean'] + ' - ' + df['ic'].fillna('') + ' - ' + df['description']
        table = KEY_TO_TABLE['PO_report_NEW_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, 'PO_report_NEW_AG', columns=TABLE_COLUMNS.get(table))

    if files.get('BO_report_AG'):
        df = pd.read_excel(files['BO_report_AG'])
        df.columns = ['ean', 'description', 'ic', 'uom', 'stock', 'in_outbound', 'reserved', 'available']
        df = df.drop(labels=(df.index.stop - 1), axis='index')
        df = df.drop(labels=[0]).dropna(subset=['ean']).reset_index(drop=True)
        df['sku'] = df['ean'].fillna('') + df['ic'].fillna('')
        table = KEY_TO_TABLE['BO_report_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, 'BO_report_AG', columns=TABLE_COLUMNS.get(table))

    if files.get('1C_packing_AG'):
        df = pd.read_excel(files['1C_packing_AG'])
        df = _drop_trailing_total(df)
        df.columns = [
            'pack_type', 'is_dimensionless', 'weight_uom', 'height_uom', 'depth_uom', 'unit',
            'dims_repr', 'volume_uom', 'size_type', 'width_uom', 'tare_characteristic', 'measure_type',
            'full_name', 'intl_abbr', 'package_type', 'accounting_type', 'processing_multiplicity',
            'axelot_guid', 'ic', 'ean', 'is_indivisible', 'pack_level', 'gross_weight', 'height',
            'depth', 'numerator', 'denominator', 'volume', 'width', 'packs_qty', 'layers_per_pallet',
            'transport_boxes_per_pallet'
        ]
        table = KEY_TO_TABLE['1C_packing_AG']
        df = _align_columns(df, TABLE_COLUMNS.get(table, []))
        table_to_file[table] = _export_df(df, out_dp, '1C_packing_AG', columns=TABLE_COLUMNS.get(table))

    return json.dumps(table_to_file)


