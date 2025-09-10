import csv
import json
import os
import uuid
import pandas as pd
import logging

from process_si_1c_to_pg.libs.mapping import SiFieldsMap
from process_si_1c_to_pg.libs.common import (
    drop_trailing_total,
    read_excel_with_multiindex,
    flatten_columns,
    normalize_types,
)

def transform_data(in_fp: str, out_dp: str, src_map: dict, dest_map: dict, file_key: str, skiprows: int, header_spec=None) -> str:
    if not src_map or not dest_map:
        logging.error(f"Mapping for {file_key} is empty. Cannot transform.")
        raise ValueError(f"Empty mapping for {file_key}")

    df = read_excel_with_multiindex(in_fp, header_spec if header_spec is not None else [0, 1, 2, 3])

    df = drop_trailing_total(df)
    df.columns = flatten_columns(df.columns)

    cleaned_src_map = {str(k).strip(): v for k, v in src_map.items()}
    df.rename(columns=cleaned_src_map, inplace=True)

    dest_columns = list(dest_map.keys())
    df = df[df.columns.intersection(dest_columns)]

    for col in dest_columns:
        if col not in df.columns:
            df[col] = None

    df = normalize_types(df, dest_map)

    if df.empty or df[dest_columns].dropna(how='all').empty:
        logging.error("No data after normalization for %s.", file_key)
        raise ValueError(f"No data after normalization for {file_key}")

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


def transform_si_data(downloaded_files_json: str, out_dp: str):
    downloaded_files = json.loads(downloaded_files_json)
    transformed_filepaths = {}

    transform_params = {
        "stock_1c": {"src_map": SiFieldsMap.stock_1c_src_map, "dest_map": SiFieldsMap.stock_1c_dest_map, "skiprows": 0, "header": [0, 1, 2, 3]},
        "open_po_ic": {"src_map": SiFieldsMap.open_po_ic_src_map, "dest_map": SiFieldsMap.open_po_ic_dest_map, "skiprows": 0, "header": [0, 1]},
        "transit": {"src_map": SiFieldsMap.transit_src_map, "dest_map": SiFieldsMap.transit_dest_map, "skiprows": 0, "header": 0},
        "stock_for_customer": {"src_map": SiFieldsMap.stock_for_customer_src_map, "dest_map": SiFieldsMap.stock_for_customer_dest_map, "skiprows": 0, "header": [0, 1, 2, 3]},
    }

    for key, params in transform_params.items():
        in_fp = downloaded_files.get(key)
        if in_fp:
            transformed_fp = transform_data(
                in_fp=in_fp,
                out_dp=out_dp,
                src_map=params["src_map"](),
                dest_map=params["dest_map"](),
                file_key=key,
                skiprows=params["skiprows"],
                header_spec=params.get("header")
            )
            if transformed_fp:
                transformed_filepaths[key] = transformed_fp
        
    return json.dumps(transformed_filepaths)
