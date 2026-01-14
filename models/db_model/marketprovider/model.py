import sqlalchemy as sa

from db_model.constants import MARKETPROVIDER_SCHEMA
from db_model.main import Base


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': MARKETPROVIDER_SCHEMA}

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in sa.inspect(self).mapper.column_attrs}


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
    name = sa.Column(sa.String)
    category_id = sa.Column(sa.Integer, index=True)
    product_photo_url = sa.Column(sa.String)
    status_id = sa.Column(sa.Integer)
    created_on = sa.Column(sa.TIMESTAMP(timezone=True))
    updated_on = sa.Column(sa.TIMESTAMP(timezone=True))