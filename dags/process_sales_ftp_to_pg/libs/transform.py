import re
import pandas as pd


QUOTE_CHARS_PATTERN = re.compile(r"[\'\"`«»‘’“”„‹›]")


def transform_sales_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].astype(str).str.replace('00-', '', regex=False)

    if 'customer' in df.columns:
        df['customer'] = (
            df['customer']
            .astype(str)
            .apply(lambda v: QUOTE_CHARS_PATTERN.sub('', v) if v is not None else v)
        )

    if 'bu' in df.columns:
        bu_map = {
            'L9 - Lamps CC': 'Lamps CC',
            'L6 - Trad. Lamps': 'TRAD',
            'L4 - LUM': 'LUM',
            'L7 - LED Lamps': 'LED',
            'L8 - CM CS': 'CM CS',
            'L5 - ECS': 'ECS',
        }
        df['bu'] = df['bu'].replace(bu_map)

    return df


