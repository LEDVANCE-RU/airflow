import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

from db_model.constants import NATIONAL_CATALOGUE_SCHEMA
from db_model.main import Base


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': NATIONAL_CATALOGUE_SCHEMA}

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in sa.inspect(self).mapper.column_attrs}

    @classmethod
    def get_update_set_for_upsert(cls, insert_stmt, exclude_cols: list):
        return {
            c.key: getattr(insert_stmt.excluded, c.key)
            for c in sa.inspect(cls).mapper.column_attrs
            if c not in exclude_cols
        }


class Product(AbstractBaseModel):
    __tablename__ = "products"

    id = sa.Column(sa.BigInteger, primary_key=True)
    good_id = sa.Column(sa.BigInteger, index=True)
    source_json = sa.Column(sa_pg.JSONB)
    etag = sa.Column(sa.String, index=True)
    name = sa.Column(sa.String)
    gtin = sa.Column(sa.String)
    tnved = sa.Column(sa.String)
    okpd2 = sa.Column(sa.String)
    conf_cert = sa.Column(sa.String)
    conf_decl_cert = sa.Column(sa.String)
    status = sa.Column(sa.String)
    is_signed = sa.Column(sa.Boolean)
    variable_characteristics = sa.Column(sa.String)
    created_at = sa.Column(sa.DateTime(timezone=True))
    updated_at = sa.Column(sa.DateTime(timezone=True))
    synced_at = sa.Column(sa.DateTime(timezone=True))
