import logging
from datetime import datetime
from math import ceil

from api.marketprovider.client import MarketProviderApiClient
from api.marketprovider.mapping import CategoryField, ProductField, ProductAttrName
from constants import TZ_UTC
from db_model.db_broker import DbBroker
from db_model.marketprovider.model import Category, Product, TempProduct
from sync_marketprovider_data_to_pg.libs.constants import LAST_SYNC_KEY, PRODUCTS_FULL_LIMIT, PRODUCTS_SHORT_LIMIT


def sync_categories(marketprovider_api_token):
    mp = MarketProviderApiClient(marketprovider_api_token)
    db_broker = DbBroker()
    current_sync_dt = datetime.now(TZ_UTC)

    for batch in mp.get_categories(paginate=True, limit=1000):
        items = batch[CategoryField.ITEMS]
        categories = [
            Category(
                id=c[CategoryField.ID],
                parent_id=c[CategoryField.PARENT_ID],
                name=c[CategoryField.NAME],
                level=c[CategoryField.LEVEL],
                status=c[CategoryField.STATUS],
                synced_at=current_sync_dt
            ) for c in items
        ]
        db_broker.upsert_marketprovider_categories(categories)
        logging.info('%s categories synced.', len(categories))


def sync_products(marketprovider_api_token: str, category_ids: list):
    mp = MarketProviderApiClient(marketprovider_api_token)
    db_broker = DbBroker()
    last_sync = db_broker.get_runtime_state(LAST_SYNC_KEY)
    last_sync_dt = last_sync.value_ts if last_sync else datetime(2000, 1, 1, tzinfo=TZ_UTC)

    for category_id in category_ids:
        for batch in mp.get_products_short(category_id, paginate=True, limit=PRODUCTS_SHORT_LIMIT):
            product_ids = []
            items = batch[ProductField.ITEMS]
            for item in items:
                updated_at = datetime.fromisoformat(item[ProductField.UPDATED_AT])
                if updated_at > last_sync_dt:
                    product_ids.append(item[ProductField.ID])
            products_full_batches_num = ceil(len(product_ids) / PRODUCTS_FULL_LIMIT)
            if not product_ids:
                continue
            for n in range(0, products_full_batches_num):
                start_idx = n * PRODUCTS_FULL_LIMIT
                end_idx = (n + 1) * PRODUCTS_FULL_LIMIT
                _sync_products_full_data(mp, db_broker, category_id, product_ids[start_idx:end_idx])


def _sync_products_full_data(mp: MarketProviderApiClient, db_broker: DbBroker,
                             category_id: int, product_ids: list[int]):
    current_sync_dt = datetime.now(TZ_UTC)
    products_full = mp.get_products_full(category_id, product_ids=product_ids, limit=PRODUCTS_FULL_LIMIT)
    with db_broker.session.bind.begin() as conn:
        db_broker.create_temp_products_table(conn)
        products = [
            TempProduct(
                id=p[ProductField.ID],
                status_id=p[ProductField.STATUS_ID],
                category_id=p[ProductField.CATEGORY][ProductField.CATEGORY_ID],
                name=_get_attr_value(p, ProductAttrName.NAME),
                brand_name=_get_attr_value(p, ProductAttrName.BRAND_NAME),
                main_image_url=_get_attr_value(p, ProductAttrName.MAIN_IMAGE),
                predecessor=_get_attr_value(p, ProductAttrName.PREDECESSOR),
                origin_country=_get_attr_value(p, ProductAttrName.ORIGIN_COUNTRY),
                inner_code = _get_attr_value(p, ProductAttrName.INNER_CODE),
                vendor_code = _get_attr_value(p, ProductAttrName.VENDOR_CODE),
                ean_upc = _get_attr_value(p, ProductAttrName.EAN_UPC),
                series_l4l=_get_attr_value(p, ProductAttrName.SERIES_L4L),
                marketing_name=_get_attr_value(p, ProductAttrName.MARKETING_NAME),
                marketing_series=_get_attr_value(p, ProductAttrName.MARKETING_SERIES),
                bulb=_get_attr_value(p, ProductAttrName.BULB),
                housing_material=_get_attr_value(p, ProductAttrName.HOUSING_MATERIAL),
                lamp_type=_get_attr_value(p, ProductAttrName.LAMP_TYPE),
                lamp_cap=_get_attr_value(p, ProductAttrName.LAMP_CAP),
                housing_color=_get_attr_value(p, ProductAttrName.HOUSING_COLOR),
                mounting_type=_get_attr_value(p, ProductAttrName.MOUNTING_TYPE),
                power=_get_attr_value(p, ProductAttrName.POWER),
                voltage=_get_attr_value(p, ProductAttrName.VOLTAGE),
                color_temperature=_get_attr_value(p, ProductAttrName.COLOR_TEMPERATURE),
                luminous_flux=_get_attr_value(p, ProductAttrName.LUMINOUS_FLUX),
                dimmable=_get_attr_value(p, ProductAttrName.DIMMABLE),
                beam_angle=_get_attr_value(p, ProductAttrName.BEAM_ANGLE),
                color_rendering_index=_get_attr_value(p, ProductAttrName.COLOR_RENDERING_INDEX),
                ip_class=_get_attr_value(p, ProductAttrName.IP_CLASS),
                lifespan=_get_attr_value(p, ProductAttrName.LIFESPAN),
                warranty_period=_get_attr_value(p, ProductAttrName.WARRANTY_PERIOD),
                cert_004_num=_get_attr_value(p, ProductAttrName.CERT_004_NUM),
                cert_037_num=_get_attr_value(p, ProductAttrName.CERT_037_NUM),
                created_at=datetime.fromisoformat(p[ProductField.CREATED_AT]),
                updated_at=datetime.fromisoformat(p[ProductField.UPDATED_AT]),
                synced_at=current_sync_dt,
                main_image_synced=False
            ) for p in products_full['items']
        ]
        db_broker.upload_marketprovider_temp_products(conn, products)
        db_broker.upsert_marketprovider_products_from_temp_table(conn)
        logging.info('%s products of category ID %s synced.', len(products), category_id)


def _get_attr_value(item: dict, group_attr_name: tuple):
    groups = item.get(ProductField.GROUPS, dict())
    group = next(
        (
            g[ProductField.GROUP_ATTRIBUTES]
            for g in groups
            if g.get(ProductField.GROUP_NAME) == group_attr_name[0]
        ),
        dict()
    )
    attr = next((a for a in group if a.get(ProductField.ATTRIBUTE_NAME) == group_attr_name[1]), dict())
    values = attr.get(ProductField.ATTRIBUTE_VALUES)
    return values[0] if values else None
