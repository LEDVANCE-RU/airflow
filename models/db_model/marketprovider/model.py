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
    main_image_url = sa.Column(sa.String)
    predecessor = sa.Column(sa.String)
    origin_country = sa.Column(sa.String)
    inner_code = sa.Column(sa.String)
    vendor_code = sa.Column(sa.String)
    ean_upc = sa.Column(sa.String)
    series = sa.Column(sa.String)
    marketing_name = sa.Column(sa.String)
    bulb = sa.Column(sa.String)
    housing_material = sa.Column(sa.String)
    lamp_cap = sa.Column(sa.String)
    power = sa.Column(sa.Numeric)
    voltage = sa.Column(sa.Text)
    color_temperature = sa.Column(sa.String)
    luminous_flux = sa.Column(sa.Numeric)
    dimmable = sa.Column(sa.String)
    beam_angle = sa.Column(sa.String)
    color_rendering_index = sa.Column(sa.String)
    lifespan = sa.Column(sa.Numeric)
    warranty_period = sa.Column(sa.Numeric)
    created_at = sa.Column(sa.DateTime(timezone=True))
    updated_at = sa.Column(sa.DateTime(timezone=True))

    synced_at = sa.Column(sa.DateTime(timezone=True))
    main_image_downloaded = sa.Column(sa.Boolean, default=False, index=True)
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