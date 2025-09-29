import csv
import os
import uuid
import pandas as pd
import logging

from .constants import TABLE_COLUMNS


def export_df(df: pd.DataFrame, out_dp: str, key: str, table: str) -> str:
    export_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}_{key}.csv")
    df.to_csv(export_fp,
              index=False,
              encoding='utf-8',
              sep=',',
              quotechar='"',
              quoting=csv.QUOTE_MINIMAL,
              columns=TABLE_COLUMNS.get(table))
    logging.info("Exported %s rows to %s", len(df), export_fp)
    return export_fp


def drop_trailing_total(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0:
        return df
    last = df.tail(1)
    has_total = last.astype(str).apply(lambda s: s.str.contains('total', case=False, na=False)).any(axis=None)
    if has_total:
        return df.iloc[:-1]
    return df


def align_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    dest_columns = TABLE_COLUMNS.get(table, [])
    if not dest_columns:
        return df
    present = [c for c in df.columns if c in dest_columns]
    df = df[present]
    for col in dest_columns:
        if col not in df.columns:
            df[col] = None
    return df[dest_columns]
