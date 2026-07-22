import re
import pandas as pd
from process_sales_ic_ftp_to_pg.libs.mapping import SalesFieldsMap

QUOTE_CHARS_PATTERN = re.compile(r"[\'\"`«»‘’“”„‹›]")


def transform_sales_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.rename(columns=SalesFieldsMap.src_map(), inplace=True)
    dest_columns = SalesFieldsMap.dest_columns()
    df = df.reindex(columns=dest_columns)

    df = df[df['period'] != 'Итого']
    df = df[df['niv'].notna()]
    df = df[df['ic'].notna()]

    if 'ean' in df.columns:
        df['ean'] = df['ean'].str.strip()
    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].str.strip()

    if 'period' in df.columns:
        df['period'] = pd.to_datetime(df['period'], errors='coerce', dayfirst=True).dt.date.astype('string')

    if 'customer' in df.columns:
        df['customer'] = df['customer'].str.replace(QUOTE_CHARS_PATTERN, '', regex=True)

    return df


