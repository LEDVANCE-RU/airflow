from enum import unique

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

from db_model.constants import MAIN_SCHEMA
from db_model.main import Base
from db_model.types import NullableUUID


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': MAIN_SCHEMA}

    @classmethod
    def get_update_set_for_upsert(cls, insert_stmt, exclude_cols: list):
        return {
            c.key: getattr(insert_stmt.excluded, c.key)
            for c in sa.inspect(cls).mapper.column_attrs
            if c not in exclude_cols
        }


class ZeroedStockHistory(AbstractBaseModel):
    __tablename__ = "zeroed_stock_history"

    UQ_CONSTR_NAME = f'uq_{__tablename__}'

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    last_seen_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    zeroed_before = sa.Column(sa.TIMESTAMP(timezone=True), index=True, nullable=False)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), index=True, nullable=False, default=sa.func.now())
    nomenclature_uuid = sa.Column(UUID(as_uuid=True))
    ic_uuid = sa.Column(NullableUUID(as_uuid=True))
    article = sa.Column(sa.String)
    ic = sa.Column(sa.String)
    yet_to_arrive_qty = sa.Column(sa.Numeric)
    receipt_in_progress_qty = sa.Column(sa.Numeric)

    __table_args__ = (
        sa.UniqueConstraint(last_seen_at, nomenclature_uuid, ic_uuid,
                            name=UQ_CONSTR_NAME),
        AbstractBaseModel.__table_args__
    )


class LemanaProOrder(AbstractBaseModel):
    __tablename__ = "lemana_pro_orders"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    order_num = sa.Column(sa.String, nullable=False)
    received_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    contents_json = sa.Column(JSONB)
    uploaded_at = sa.Column(sa.TIMESTAMP(timezone=True), index=True)


class RuntimeState(AbstractBaseModel):
    __tablename__ = "runtime_states"

    key = sa.Column(sa.String, primary_key=True)
    value_str = sa.Column(sa.String)
    value_ts = sa.Column(sa.TIMESTAMP(timezone=True))
    updated_on = sa.Column(sa.TIMESTAMP(timezone=True))


class SKUActiveCertificatesPivoted(AbstractBaseModel):
    __tablename__ = "sku_active_certificates_pivoted"

    UQ_CONSTR_NAME = f'uq_{__tablename__}'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    sku_uuid = sa.Column(UUID(as_uuid=True))
    sku_code = sa.Column(sa.String, nullable=False)
    sku = sa.Column(sa.String)
    conf_cert_name = sa.Column(sa.String)
    conf_cert_num = sa.Column(sa.String)
    conf_cert_active_from = sa.Column(sa.Date)
    conf_cert_active_till = sa.Column(sa.Date)
    conf_decl_cert_name = sa.Column(sa.String)
    conf_decl_cert_num = sa.Column(sa.String)
    conf_decl_cert_active_from = sa.Column(sa.Date)
    conf_decl_cert_active_till = sa.Column(sa.Date)

    __table_args__ = (
        sa.UniqueConstraint(sku_uuid, name=UQ_CONSTR_NAME),
        AbstractBaseModel.__table_args__
    )
