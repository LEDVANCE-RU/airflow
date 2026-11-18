import itertools

import pandas

from db_model.constants import PackageLevels
from db_model.db_broker import DbBroker
from db_model.mapping import QueryIcPackageMap


def process_data(result_fp: str) -> pandas.DataFrame | None:
    fmap = QueryIcPackageMap

    with (DbBroker() as db_broker):
        stmt = db_broker.get_actual_ic_packages(return_stmt=True)

        excluded_levels = [PackageLevels.PCE]
        included_levels = list(p for p in PackageLevels if p not in excluded_levels)

        df = pandas.read_sql(stmt, db_broker.session.connection())
        df = df[df[fmap.LEVEL].isin(included_levels)]

        unique_key = [fmap.IC_UUID, fmap.LEVEL]
        df.drop_duplicates(unique_key, inplace=True)

        index_cols = [
            fmap.IC_UUID,
            fmap.SKU_UUID,
            fmap.SKU,
            fmap.NAME,
            fmap.FULL_NAME,
            fmap.IC,
            fmap.IC_LIFECYCLE_STATUS,
            fmap.TNVED,
            fmap.OKPD2,
            fmap.CONTAINS_BATTERY,
            fmap.SUPPLIER_NAME
        ]
        key_cols = [fmap.LEVEL]
        value_cols = [
            fmap.EAN,
            fmap.QTY,
            fmap.LENGTH,
            fmap.WIDTH,
            fmap.HEIGHT,
            fmap.WEIGHT
        ]
        df = df.pivot(
            index=index_cols,
            columns=key_cols,
            values=value_cols
        ).reset_index()

        # flatten the columns
        df.columns = ['_'.join([c for c in col[::-1] if c]).lower() for col in df.columns.values]
        target_cols = (
            index_cols + list(
                f"{p[0]}_{p[1]}".lower()
                for p in itertools.product(included_levels, value_cols)
            )
        )
        # forcibly add columns from mapping for the case when levels are missing in source dataframe
        df = df.reindex(columns=target_cols)
        df = mixin_certificates_data(df)
        df.drop([fmap.IC_UUID, fmap.SKU_UUID], axis=1, inplace=True)
        df.to_excel(result_fp, index=False)

def mixin_certificates_data(df: pandas.DataFrame) -> pandas.DataFrame:
    from db_model.core.model import SKUActiveCertificatesPivoted

    fmap = QueryIcPackageMap

    cert_cols = [
        SKUActiveCertificatesPivoted.sku_uuid.name,
        SKUActiveCertificatesPivoted.conf_cert_num.name,
        SKUActiveCertificatesPivoted.conf_decl_cert_num.name
    ]

    with (DbBroker() as db_broker):
        stmt = db_broker.get_pivoted_sku_active_certificates(return_stmt=True)
        df_cert = pandas.read_sql(stmt, db_broker.session.connection(), columns=cert_cols)

    df_result = df.merge(
        df_cert,
        how='left',
        left_on=fmap.SKU_UUID,
        right_on=SKUActiveCertificatesPivoted.sku_uuid.name
    )

    col1 = df_result.pop(SKUActiveCertificatesPivoted.conf_cert_num.name)
    col2 = df_result.pop(SKUActiveCertificatesPivoted.conf_decl_cert_num.name)
    target_idx = df_result.columns.get_loc(fmap.IC_LIFECYCLE_STATUS) + 1
    df_result.insert(target_idx, SKUActiveCertificatesPivoted.conf_decl_cert_num.name, col2)
    df_result.insert(target_idx, SKUActiveCertificatesPivoted.conf_cert_num.name, col1)

    return df_result
