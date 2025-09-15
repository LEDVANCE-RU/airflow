import logging
import pandas as pd
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
        return pd.read_excel(in_fp, header=header_spec, engine='openpyxl', dtype=str)
    except Exception as e:
        logging.error("Could not read Excel file %s with header=%s: %s", in_fp, header_spec, e)
        raise


def flatten_columns(columns) -> list[str]:
    if not isinstance(columns, pandas.MultiIndex):
        return columns
    pattern = re.compile(r'Unnamed: \d+_level_\d+')
    renamed_cols = []
    for col in columns:
        renamed_cols.append('.'.join(filter(None, [re.sub(pattern, '', subcol) 
                                                   for subcol in col])))
    return renamed_cols


def normalize_types(df: pd.DataFrame, dest_map: dict) -> pd.DataFrame:
    date_columns = [k for k, v in dest_map.items() if 'DATE' in v.type.upper()]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col].astype(str).str.strip(), errors='coerce', dayfirst=True).dt.date
    int_columns = [k for k, v in dest_map.items() if 'INT' in v.type.upper()]
    for col in int_columns:
        if col in df.columns:
            s = df[col].fillna('').astype(str)
            s = s.str.replace('\u00a0', '', regex=False)
            s = s.str.replace(' ', '', regex=False)
            s = s.str.replace(',', '.', regex=False)
            s = s.str.replace(r'[^0-9\.\-+]', '', regex=True)
            nums = pd.to_numeric(s, errors='coerce')
            df[col] = nums.round(0).astype('Int64')
    return df
