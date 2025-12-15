import csv
import os
import re
import uuid
import pandas as pd
import logging

from process_report_sources_to_pg.libs.constants import TABLE_COLUMNS


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
    return df.reindex(columns=dest_columns)


def extract_period_from_header(filepath: str) -> tuple[str, str]:
    header_df = pd.read_excel(filepath, nrows=7)
    df_str = header_df.to_string(index=False)
    date_pattern = r'\d{2}\.\d{2}\.\d{4}'
    match_dates = re.search(fr'Период: ({date_pattern}) - ({date_pattern})', df_str)
    if match_dates:
        from_, to_ = match_dates.groups()[:2]
        return from_, to_
    else:
        raise ValueError(f"Не удалось определить период в заголовке отчета '{os.path.basename(filepath)}'")