import pandas as pd
from process_sales_ic_ftp_to_pg.libs.on_delivery_mapping import OnDeliveryFieldsMap


def transform_on_delivery_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.rename(columns=OnDeliveryFieldsMap.src_map(), inplace=True)
    df = df.reindex(columns=OnDeliveryFieldsMap.dest_columns())

    if 'ean' in df.columns:
        df['ean'] = df['ean'].str.strip()

    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].str.strip()

    if 'ownership_transfer_date' in df.columns:
        df['ownership_transfer_date'] = pd.to_datetime(
            df['ownership_transfer_date'], errors='coerce', dayfirst=True
        ).dt.date.astype('string')

    return df
