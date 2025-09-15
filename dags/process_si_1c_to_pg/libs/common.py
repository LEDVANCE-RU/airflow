import logging
import pandas as pd
import re
from typing import Any


def drop_trailing_total(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0:
        return df
    last = df.tail(1)
    has_total = last.astype(str).apply(lambda s: s.str.contains('total', case=False, na=False)).any(axis=None)
    if has_total:
        return df.iloc[:-1]
    return df


def read_excel_with_multiindex(in_fp: str, header_spec: Any) -> pd.DataFrame:
    try:
        # Let pandas/openpyxl infer native types from Excel cells
        return pd.read_excel(in_fp, header=header_spec, engine='openpyxl')
    except Exception as e:
        logging.error("Could not read Excel file %s with header=%s: %s", in_fp, header_spec, e)
        raise


def flatten_columns(columns) -> list[str]:
    if not isinstance(columns, pd.MultiIndex):
        return columns
    pattern = re.compile(r'Unnamed: \d+_level_\d+')
    renamed_cols = []
    for col in columns:
        renamed_cols.append('.'.join(filter(None, [re.sub(pattern, '', subcol) 
                                                   for subcol in col])))
    return renamed_cols
