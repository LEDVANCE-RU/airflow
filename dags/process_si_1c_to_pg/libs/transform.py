import csv
import json
import os
import uuid
import pandas as pd
import logging

from process_si_1c_to_pg.libs.mapping import SiFieldsMap

def transform_data(in_fp: str, out_dp: str, src_map: dict, dest_map: dict, file_key: str, skiprows: int) -> str | None:
    if not src_map or not dest_map:
        logging.warning(f"Mapping for {file_key} is empty. Skipping transformation.")
        return None

    try:
        df = pd.read_csv(in_fp, sep='\\t', skiprows=skiprows, engine='python')
    except Exception as e:
        logging.error(f"Could not read file {in_fp}: {e}")
        return None

    df.rename(columns=src_map, inplace=True)
    dest_columns = list(dest_map.keys())
    df = df[df.columns.intersection(dest_columns)]

    for col in dest_columns:
        if col not in df.columns:
            df[col] = None

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
        "stock_1c": {"src_map": SiFieldsMap.stock_1c_src_map, "dest_map": SiFieldsMap.stock_1c_dest_map, "skiprows": 2},
        "open_po_ic": {"src_map": SiFieldsMap.open_po_ic_src_map, "dest_map": SiFieldsMap.open_po_ic_dest_map, "skiprows": 0},
        "transit": {"src_map": SiFieldsMap.transit_src_map, "dest_map": SiFieldsMap.transit_dest_map, "skiprows": 0},
        "stock_for_customer": {"src_map": SiFieldsMap.stock_for_customer_src_map, "dest_map": SiFieldsMap.stock_for_customer_dest_map, "skiprows": 0},
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
                skiprows=params["skiprows"]
            )
            if transformed_fp:
                transformed_filepaths[key] = transformed_fp
        
    return json.dumps(transformed_filepaths) 