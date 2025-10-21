import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from db_model.constants import MAIN_SCHEMA
from db_model.main import Base
from db_model.types import NullableUUID


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': MAIN_SCHEMA}


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
    receipt_in_progress_qty = sa.Column(sa.Numeric)

    __table_args__ = (
        sa.UniqueConstraint(last_seen_at, nomenclature_uuid, ic_uuid,
                            name=UQ_CONSTR_NAME),
        AbstractBaseModel.__table_args__
    )
