import csv
import json
import os
import uuid
import pandas as pd
import logging

from process_md_1c_to_pg.libs.mapping import MdFieldsMap

def transform_data(in_fp: str, out_dp: str, src_map: dict, dest_map: dict, file_key: str) -> str:
    # def to_decimal(v):
    #     return pd.to_numeric(str(v).replace('\xa0', '').replace(' ', '').replace(',', '.'), errors='coerce')
    # price_src = {v: k for k, v in src_map.items()}.get('price_federal_wo_vat')
    # df = pd.read_excel(in_fp, dtype=str, converters={price_src: to_decimal} if price_src else None)
    df = pd.read_excel(in_fp, dtype=str)
    df.rename(columns=src_map, inplace=True)
    dest_columns = list(dest_map.keys())
    df = df[df.columns.intersection(dest_columns)]
    for col in dest_columns:
        if col not in df.columns:
            df[col] = None
    df = df[dest_columns]

    if 'ic' in df.columns:
        df = df[df['ic'].fillna('').astype(str).str.strip().ne('')]
    if 'items_type' in df.columns:
        df = df[~df['items_type'].astype(str).str.strip().str.lower().isin(['total', 'итого'])]

    if 'deletion_mark' in df.columns:
        df['deletion_mark'] = (
            df['deletion_mark']
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(['true', '1', 'да', 'y', 'yes'])
        )
    
    # if 'ean' in df.columns:
    #     df['ean'] = (
    #         df['ean'].astype(str)
    #         .str.replace(r'\.0$', '', regex=True)
    #         .str.replace(r'\D', '', regex=True)
    #     )
    
    # if 'priority' in df.columns:
    #     df['priority'] = (
    #         df['priority'].astype(str)
    #         .str.replace(r'[^0-9\-+]', '', regex=True)
    #         .str.extract(r'([+-]?\d+)', expand=False)
    #     )

    export_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}_{file_key}.csv")
    df.to_csv(export_fp,
              index=False,
              encoding='utf-8',
              sep=',',
              quotechar='"',
              quoting=csv.QUOTE_MINIMAL,
              columns=dest_columns)
    
    logging.info(f"Transformed {in_fp} to {export_fp}")
    return export_fp


def transform_md_data(downloaded_files_json: str, out_dp: str):
    downloaded_files = json.loads(downloaded_files_json)
    transformed_filepaths = {}
    products_in_fp = downloaded_files.get("products")

    if products_in_fp:
        transformed_filepaths["products"] = transform_data(
            in_fp=products_in_fp,
            out_dp=out_dp,
            src_map=MdFieldsMap.products_src_map(),
            dest_map=MdFieldsMap.products_dest_map(),
            file_key="products"
        )

    pricelist_in_fp = downloaded_files.get("price_list")
    if pricelist_in_fp:
        transformed_filepaths["price_list"] = transform_data(
            in_fp=pricelist_in_fp,
            out_dp=out_dp,
            src_map=MdFieldsMap.pricelist_src_map(),
            dest_map=MdFieldsMap.pricelist_dest_map(),
            file_key="price_list"
        )
        
    return json.dumps(transformed_filepaths)
