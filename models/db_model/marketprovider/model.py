import sqlalchemy as sa
from sqlalchemy import Table

from db_model.constants import MARKETPROVIDER_SCHEMA
from db_model.main import Base, metadata


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': MARKETPROVIDER_SCHEMA}

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in sa.inspect(self).mapper.column_attrs}

    @classmethod
    def get_update_set_for_upsert(cls, insert_stmt, exclude_cols: list):
        return {
            c.key: getattr(insert_stmt.excluded, c.key)
            for c in sa.inspect(cls).mapper.column_attrs
            if c not in exclude_cols
        }


class Category(AbstractBaseModel):
    __tablename__ = "categories"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    level = sa.Column(sa.Integer)
    parent_id = sa.Column(sa.Integer, index=True)
    status = sa.Column(sa.String)

    synced_at = sa.Column(sa.DateTime(timezone=True))


class Product(AbstractBaseModel):
    __tablename__ = "products"

    id = sa.Column(sa.Integer, primary_key=True)
    status_id = sa.Column(sa.Integer)
    category_id = sa.Column(sa.Integer, index=True)
    name = sa.Column(sa.String)
    brand_name = sa.Column(sa.String)
    main_image_url = sa.Column(sa.String)
    predecessor = sa.Column(sa.String)
    warehouse_status = sa.Column(sa.String)
    lifecycle_status = sa.Column(sa.String)
    origin_country = sa.Column(sa.String)
    inner_code = sa.Column(sa.String)
    vendor_code = sa.Column(sa.String)
    ean_upc = sa.Column(sa.String)
    marketing_name = sa.Column(sa.String)
    marketing_series = sa.Column(sa.String)
    series_l4l = sa.Column(sa.String)
    bulb = sa.Column(sa.String)
    housing_material = sa.Column(sa.String)
    lamp_type = sa.Column(sa.String)
    lamp_cap = sa.Column(sa.String)
    housing_color = sa.Column(sa.String)
    diffuser_type = sa.Column(sa.String)
    mounting_type = sa.Column(sa.String)
    power = sa.Column(sa.Numeric)
    voltage = sa.Column(sa.Text)
    color_temperature = sa.Column(sa.String)
    luminous_flux = sa.Column(sa.Numeric)
    dimmable = sa.Column(sa.String)
    beam_angle = sa.Column(sa.String)
    color_rendering_index = sa.Column(sa.String)
    ip_class = sa.Column(sa.String)
    lifespan = sa.Column(sa.Numeric)
    warranty_period = sa.Column(sa.Numeric)
    cert_004_num = sa.Column(sa.String)
    cert_037_num = sa.Column(sa.String)
    created_at = sa.Column(sa.DateTime(timezone=True))
    updated_at = sa.Column(sa.DateTime(timezone=True))

    pce_in_indivisible_pkg = sa.Column(sa.Numeric)
    order_multiple_qty = sa.Column(sa.Numeric)
    order_min_qty = sa.Column(sa.Numeric)

    pce_on_pallet = sa.Column(sa.Numeric)
    individual_pkg_length = sa.Column(sa.Numeric)
    individual_pkg_height = sa.Column(sa.Numeric)
    individual_pkg_width = sa.Column(sa.Numeric)
    individual_pkg_weight = sa.Column(sa.Numeric)
    transport_pkg_length = sa.Column(sa.Numeric)
    transport_pkg_height = sa.Column(sa.Numeric)
    transport_pkg_width = sa.Column(sa.Numeric)
    transport_pkg_weight = sa.Column(sa.Numeric)
    pce_in_transport_pkg = sa.Column(sa.Numeric)

    diameter = sa.Column(sa.Numeric)
    length = sa.Column(sa.Numeric)
    width = sa.Column(sa.Numeric)
    height = sa.Column(sa.Numeric)
    weight = sa.Column(sa.Numeric)

    synced_at = sa.Column(sa.DateTime(timezone=True))
    main_image_synced = sa.Column(sa.Boolean, default=False, index=True)
    main_image_relpath = sa.Column(sa.String)


temp_products_table = Table(
    'temp_products',
    metadata,
    *[sa.Column(c.name, c.type, primary_key=c.primary_key) for c in Product.__table__.columns],
    prefixes=['TEMPORARY'],
    postgresql_on_commit='DROP'
)


class TempProduct(AbstractBaseModel):
    __table__ = temp_products_table