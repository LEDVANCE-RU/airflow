import re
import pandas as pd
from process_sales_ftp_to_pg.libs.constants import BU_REPLACEMENTS
from process_sales_ftp_to_pg.libs.mapping import SalesFieldsMap

QUOTE_CHARS_PATTERN = re.compile(r"[\'\"`«»‘’“”„‹›]")


def transform_sales_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.rename(columns=SalesFieldsMap.src_map(), inplace=True)
    dest_columns = SalesFieldsMap.dest_columns()
    present = df.columns.intersection(dest_columns)
    df = df[present]
    for col in dest_columns:
        if col not in df.columns:
            df[col] = None
    df = df[dest_columns]

    if 'ean' in df.columns:
        df['ean'] = df['ean'].astype('string').str.strip()

    if 'period' in df.columns:
        df['period'] = pd.to_datetime(df['period'], errors='coerce', dayfirst=True).dt.date.astype('string')

    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].astype('string').str.replace('00-', '', regex=False).str.strip()

    if 'customer' in df.columns:
        df['customer'] = df['customer'].astype('string').str.replace(QUOTE_CHARS_PATTERN, '', regex=True)

    if 'bu' in df.columns:
        df['bu'] = df['bu'].replace(BU_REPLACEMENTS)

    return df


