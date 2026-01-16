import sqlalchemy as sa

from db_model.constants import MARKETPROVIDER_SCHEMA
from db_model.main import Base


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': MARKETPROVIDER_SCHEMA}

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in sa.inspect(self).mapper.column_attrs}

    @classmethod
    def get_update_set_for_upsert(cls, insert_stmt, index_elements: list):
        return {
            c.key: getattr(insert_stmt.excluded, c.key)
            for c in sa.inspect(cls).mapper.column_attrs
            if c not in index_elements
        }


class Category(AbstractBaseModel):
    __tablename__ = "categories"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    level = sa.Column(sa.Integer)
    parent_id = sa.Column(sa.Integer, index=True)
    status = sa.Column(sa.String)


class Product(AbstractBaseModel):
    __tablename__ = "products"

    id = sa.Column(sa.Integer, primary_key=True)
    status_id = sa.Column(sa.Integer)
    category_id = sa.Column(sa.Integer, index=True)
    name = sa.Column(sa.String)
    main_image_url = sa.Column(sa.String)
    predecessor = sa.Column(sa.String)
    origin_country = sa.Column(sa.String)
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
    created_on = sa.Column(sa.TIMESTAMP(timezone=True))
    updated_on = sa.Column(sa.TIMESTAMP(timezone=True))