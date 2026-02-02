import logging
from datetime import datetime

from api.marketprovider.client import MarketProviderApiClient
from api.marketprovider.mapping import CategoryField, ProductField, ProductAttrName
from constants import TZ_UTC
from db_model.db_broker import DbBroker
from db_model.marketprovider.model import Category, TempProduct
from sync_marketprovider_data_to_pg.libs.constants import LAST_SYNC_KEY, PRODUCTS_FULL_LIMIT


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
        for batch in mp.get_products_full(category_id, updated_since=last_sync_dt,
                                          paginate=True, limit=PRODUCTS_FULL_LIMIT):
            items = batch[ProductField.ITEMS]
            _sync_products_batch(items)
            logging.info('%s products of category ID %s synced.', len(items), category_id)


def _sync_products_batch(items: list):
    current_sync_dt = datetime.now(TZ_UTC)
    db_broker = DbBroker()
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
                marketing_name=_get_attr_value(p, ProductAttrName.MARKETING_NAME),
                marketing_series=_get_attr_value(p, ProductAttrName.MARKETING_SERIES),
                series_l4l=_get_attr_value(p, ProductAttrName.SERIES_L4L),
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
            ) for p in items
        ]
        db_broker.upload_marketprovider_temp_products(conn, products)
        db_broker.upsert_marketprovider_products_from_temp_table(conn)


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
