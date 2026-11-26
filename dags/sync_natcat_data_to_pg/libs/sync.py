import logging
from datetime import datetime
from typing import Any

import pandas

from api.national_catalogue.client import NationalCatalogueApiClient
import db_model.national_catalogue.model as m_nc
from api.national_catalogue.constants import DT_FORMAT
import api.national_catalogue.mapping as nc_map
from constants import TZ_MSK
from db_model.db_broker import DbBroker


def sync_products(natcat_api_key: str, owner_inn: int):
    _right_suffix = '_x'
    _etag_w_suffix = f'{m_nc.Product.etag.name}{_right_suffix}'

    nc = NationalCatalogueApiClient(natcat_api_key)

    with (DbBroker() as db_broker):
        for idx, resp_batch in enumerate(nc.iter_all_products(owner_inn=owner_inn)):
            logging.info('Analyzing products batch #%s...', idx)
            products = resp_batch[nc_map.ApiResponseField.RESULT][nc_map.ProductListField.PRODUCTS]
            df = pandas.DataFrame.from_records(products)
            df = df.astype(
                {
                    nc_map.ProductListItemField.GOOD_ID: 'int64',
                    nc_map.ProductListItemField.ETAG: str
                }
            )
            good_ids = df[nc_map.ProductListItemField.GOOD_ID].to_list()
            stmt = db_broker.get_natcat_products(
                good_ids=good_ids,
                columns=[m_nc.Product.good_id, m_nc.Product.etag],
                return_stmt=True
            )
            df_db = pandas.read_sql_query(stmt, db_broker.session.connection())
            df_diff = df.merge(
                df_db,
                left_on=nc_map.ProductListItemField.GOOD_ID,
                right_on=m_nc.Product.good_id.name,
                how='left',
                suffixes=('', _right_suffix)
            )
            df_diff = df_diff[
                pandas.isna(df_diff[_etag_w_suffix])
                | (df_diff[nc_map.ProductListItemField.ETAG] != df_diff[_etag_w_suffix])
            ]
            if df_diff.empty:
                logging.info('No changes found in products batch #%s', idx)
                continue
            logging.info('%s changes found in products batch #%s...', len(df_diff), idx)

            items = []
            for row_idx, row in df_diff.iterrows():
                good_id = row[nc_map.ProductListItemField.GOOD_ID]
                logging.info('Retrieving data for product with good_id=%s...', good_id)
                product_data = nc.get_product_details(good_id)[nc_map.ApiResponseField.RESULT][0]
                p = _fill_product_data(product_data, row)
                items.append(p)

            db_broker.upsert_natcat_products(items)


def _get_attr_value(data: dict, attr_name: str) -> Any:
    value = next(
        (
            a[nc_map.ProductAttrField.VALUE]
            for a in data[nc_map.ProductField.ATTRS]
            if a[nc_map.ProductAttrField.NAME] == attr_name
        ), None
    )
    return value


def _fill_product_data(data: dict, row: pandas.Series):
    p = {
        m_nc.Product.good_id.name: row[nc_map.ProductListItemField.GOOD_ID],
        m_nc.Product.source_json: data,
        m_nc.Product.etag.name: row[nc_map.ProductListItemField.ETAG],
        m_nc.Product.name.name: data[nc_map.ProductField.NAME],
        m_nc.Product.status.name: data[nc_map.ProductField.STATUS],
        m_nc.Product.is_signed.name: data[nc_map.ProductField.IS_SIGNED],
        m_nc.Product.created_at.name:
            datetime.strptime(
                data[nc_map.ProductField.CREATED_AT], DT_FORMAT
            ).replace(tzinfo=TZ_MSK),
        m_nc.Product.updated_at.name:
            datetime.strptime(
                data[nc_map.ProductField.UPDATED_AT], DT_FORMAT
            ).replace(tzinfo=TZ_MSK),
        m_nc.Product.synced_at.name: datetime.now(TZ_MSK),
        m_nc.Product.gtin.name: data[nc_map.ProductField.IDENTITY][0][nc_map.ProductIdentityField.VALUE],
        m_nc.Product.tnved.name: _get_attr_value(data, nc_map.ProductAttrName.TNVED),
        m_nc.Product.okpd2.name: _get_attr_value(data, nc_map.ProductAttrName.OKPD2),
        m_nc.Product.variable_characteristics.name:
            _get_attr_value(data, nc_map.ProductAttrName.VARCHAR),
        m_nc.Product.conf_cert.name: _get_attr_value(data, nc_map.ProductAttrName.CONF_CERT),
        m_nc.Product.conf_decl_cert.name:
            _get_attr_value(data, nc_map.ProductAttrName.CONF_DECL_CERT)
    }
    return p