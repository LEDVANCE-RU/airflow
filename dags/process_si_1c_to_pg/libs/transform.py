import csv
import json
import os
import uuid
import logging
from dataclasses import dataclass
from typing import Callable, Any
import pandas as pd

from process_si_1c_to_pg.libs.mapping import SiFieldsMap
from process_si_1c_to_pg.libs.common import (
    drop_trailing_total,
    read_excel_with_multiindex,
    flatten_columns,
)

@dataclass(frozen=True)
class DatasetSpec:
    src_map: Callable[[], dict]
    dest_map: Callable[[], dict]
    header: Any

def transform_data(in_fp: str, out_dp: str, src_map: dict, dest_map: dict, file_key: str, skiprows: int, header_spec=None) -> str:
    if not src_map or not dest_map:
        logging.error(f"Mapping for {file_key} is empty. Cannot transform.")
        raise ValueError(f"Empty mapping for {file_key}")

    header_levels = header_spec if header_spec is not None else [0, 1, 2, 3]
    cols = pd.read_excel(in_fp, header=header_levels, engine='openpyxl', nrows=0).columns
    unnested_columns = flatten_columns(cols)
    dtype_map = {i: str for i, name in enumerate(unnested_columns) if src_map.get(name) == 'ean'}

    df = read_excel_with_multiindex(
        in_fp,
        header_levels,
        dtype=dtype_map
    )

    df = drop_trailing_total(df)
    df.columns = flatten_columns(df.columns)

    cleaned_src_map = {str(k).strip(): v for k, v in src_map.items()}
    df.rename(columns=cleaned_src_map, inplace=True)

    dest_columns = list(dest_map.keys())
    df = df.reindex(columns=dest_columns)

    if 'ean' in df.columns:
        df['ean'] = df['ean'].str.strip()

    for col, field in dest_map.items():
        if 'INTEGER' in field.type:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    if df.empty or df[dest_columns].dropna(how='all').empty:
        error_message = f"No data after normalization for {file_key}"
        logging.error(error_message)
        raise ValueError(error_message)

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

    specs: dict[str, DatasetSpec] = {
        "stock_1c": DatasetSpec(SiFieldsMap.stock_1c_src_map, SiFieldsMap.stock_1c_dest_map, [0, 1, 2, 3]),
        "open_po_ic": DatasetSpec(SiFieldsMap.open_po_ic_src_map, SiFieldsMap.open_po_ic_dest_map, [0, 1]),
        "transit": DatasetSpec(SiFieldsMap.transit_src_map, SiFieldsMap.transit_dest_map, 0),
        "stock_for_customer": DatasetSpec(SiFieldsMap.stock_for_customer_src_map, SiFieldsMap.stock_for_customer_dest_map, [0, 1, 2, 3]),
    }

    for key, spec in specs.items():
        in_fp = downloaded_files.get(key)
        if in_fp:
            transformed_fp = transform_data(
                in_fp=in_fp,
                out_dp=out_dp,
                src_map=spec.src_map(),
                dest_map=spec.dest_map(),
                file_key=key,
                skiprows=0,
                header_spec=spec.header,
            )
            if transformed_fp:
                transformed_filepaths[key] = transformed_fp
        
    return json.dumps(transformed_filepaths)
