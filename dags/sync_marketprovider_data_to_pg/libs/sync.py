from datetime import datetime
from itertools import product
from math import ceil

from api.marketprovider.client import MarketProviderApiClient
from api.marketprovider.constants import CategoryField, ProductField, ProductAttrName
from constants import TZ_UTC
from db_model.db_broker import DbBroker
from db_model.marketprovider.model import Category, Product
from sync_marketprovider_data_to_pg.libs.constants import LAST_SYNC_KEY, PRODUCTS_FULL_LIMIT, PRODUCTS_SHORT_LIMIT


def sync_categories(marketprovider_api_token):
    mp = MarketProviderApiClient(marketprovider_api_token)
    db_broker = DbBroker()

    for batch in mp.get_categories(paginate=True, limit=1000):
        items = batch[CategoryField.ITEMS]
        categories = [
            Category(
                id=c[CategoryField.ID],
                parent_id=c[CategoryField.PARENT_ID],
                name=c[CategoryField.NAME],
                level=c[CategoryField.LEVEL],
                status=c[CategoryField.STATUS]
            ) for c in items
        ]
        db_broker.upsert_marketprovider_categories(categories)


def sync_products(marketprovider_api_token: str, category_ids: list):
    mp = MarketProviderApiClient(marketprovider_api_token)
    db_broker = DbBroker()
    current_sync_dt = datetime.now(TZ_UTC)
    last_sync_dt = db_broker.get_runtime_state(LAST_SYNC_KEY) or datetime(2000, 1, 1, tzinfo=TZ_UTC)

    for category_id in category_ids:
        for batch in mp.get_products_short(category_id, paginate=True, limit=PRODUCTS_SHORT_LIMIT):
            product_ids = []
            items = batch[ProductField.ITEMS]
            for item in items:
                updated_at = datetime.fromisoformat(item[ProductField.UPDATED_AT])
                if updated_at > last_sync_dt:
                    product_ids.append(item[ProductField.ID])
            products_full_batches_num = ceil(PRODUCTS_SHORT_LIMIT / PRODUCTS_FULL_LIMIT)
            for n in range(0, products_full_batches_num):
                start_idx = n * PRODUCTS_FULL_LIMIT
                end_idx = (n + 1) * PRODUCTS_FULL_LIMIT
                _sync_products_full_data(mp, db_broker, category_id, product_ids[start_idx:end_idx])


def _sync_products_full_data(mp: MarketProviderApiClient, db_broker: DbBroker,
                             category_id: int, product_ids: list[int]):
    products_full = mp.get_products_full(category_id, product_ids=product_ids, limit=PRODUCTS_FULL_LIMIT)
    try:
        products = [
            Product(
                id=p[ProductField.ID],
                status_id=p[ProductField.STATUS_ID],
                category_id=p[ProductField.CATEGORY][ProductField.CATEGORY_ID],
                name=_get_attr_value(p, ProductAttrName.NAME),
                main_image_url=_get_attr_value(p, ProductAttrName.MAIN_IMAGE),
                predecessor=_get_attr_value(p, ProductAttrName.PREDECESSOR),
                origin_country=_get_attr_value(p, ProductAttrName.ORIGIN_COUNTRY),
                series=_get_attr_value(p, ProductAttrName.SERIES),
                marketing_name=_get_attr_value(p, ProductAttrName.MARKETING_NAME),
                bulb=_get_attr_value(p, ProductAttrName.BULB),
                housing_material=_get_attr_value(p, ProductAttrName.HOUSING_MATERIAL),
                lamp_cap=_get_attr_value(p, ProductAttrName.LAMP_CAP),
                power=_get_attr_value(p, ProductAttrName.POWER),
                voltage=_get_attr_value(p, ProductAttrName.VOLTAGE),
                color_temperature=_get_attr_value(p, ProductAttrName.COLOR_TEMPERATURE),
                luminous_flux=_get_attr_value(p, ProductAttrName.LUMINOUS_FLUX),
                dimmable=_get_attr_value(p, ProductAttrName.DIMMABLE),
                beam_angle=_get_attr_value(p, ProductAttrName.BEAM_ANGLE),
                color_rendering_index=_get_attr_value(p, ProductAttrName.COLOR_RENDERING_INDEX),
                lifespan=_get_attr_value(p, ProductAttrName.LIFESPAN),
                warranty_period=_get_attr_value(p, ProductAttrName.WARRANTY_PERIOD),
                created_on=datetime.fromisoformat(p[ProductField.CREATED_AT]),
                updated_on=datetime.fromisoformat(p[ProductField.UPDATED_AT])
            ) for p in products_full['items']
        ]
    except KeyError:
        raise

    db_broker.upsert_marketprovider_products(products)


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

