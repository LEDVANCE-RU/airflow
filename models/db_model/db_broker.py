from datetime import timedelta, datetime
from functools import wraps
from typing import Callable, Any

import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
import sqlalchemy.dialects.postgresql as sa_pg
import sqlalchemy.sql as sa_sql
from sqlalchemy.orm import aliased as sa_aliased

from db_model.main import SessionLocal
from db_model.mapping import QuerySuccessorIcMap
from db_model.core.model import ZeroedStockHistory
from db_model.onec_extract.constants import WAREHOUSE_OF_GOODS_UUID
from db_model.onec_extract.model import WmsStockHistory, Nomenclature, Ic, FutureArrivalsStock


def on_failure(func: Callable) -> Callable:
    """Decorator for automatic transaction management on all DB class methods."""

    @wraps(func)
    def wrapper(self, *args, **kwargs) -> Any:
        if not hasattr(self, 'session'):
            raise AttributeError("DBBroker class must have 'session' attribute")
        session = self.session
        try:
            result = func(self, *args, **kwargs)
            return result
        except Exception:
            session.rollback()
            raise

    return wrapper


class AutoRollbackMeta(type):
    """Metaclass to automatically decorate all methods."""
    def __new__(cls, name, bases, namespace):
        for attr_name, attr_value in namespace.items():
            if callable(attr_value) and not attr_name.startswith('_'):
                namespace[attr_name] = on_failure(attr_value)
        return super().__new__(cls, name, bases, namespace)


class DbBroker(metaclass=AutoRollbackMeta):
    def __init__(self, session=None):
        self.session = session or SessionLocal()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def get_last_wms_stock_date(self) -> datetime:
        return self.session.query(sa.func.max(WmsStockHistory.period)).scalar()

    def get_wms_zeroed_stock(self, since: datetime = None, to: datetime = None,
                             *, return_stmt: bool = False) -> list[sa.engine.Row] | sa_sql.Select:
        """Get ICs with zeroed stock since given datetime + planned arrivals."""

        # actual stock datetime
        if not to:
            to = self.get_last_wms_stock_date()
        since = since or to - timedelta(days=7)

        WmsStockHistoryLast = sa_aliased(WmsStockHistory)  # type: WmsStockHistory

        # get zeroed-stock ICs since given datetime
        cte_nullified_stock = (
            sa.select(
                Nomenclature.uuid.label('nomenclature_uuid'),
                Nomenclature.article,
                Ic.uuid.label('ic_uuid'),
                Ic.name,
                WmsStockHistory.period
            ).select_from(
                WmsStockHistory
            ).join(
                Nomenclature,
                sa.and_(
                    Nomenclature.is_deleted == False,
                    sa.cast(Nomenclature.uuid, sa.String) == WmsStockHistory.nomenclature_uuid,
                )
            ).outerjoin(
                Ic,
                sa.and_(
                    Ic.is_deleted == False,
                    Ic.nomenclature_uuid == Nomenclature.uuid,
                    Ic.uuid == WmsStockHistory.ic_uuid
                )
            ).outerjoin(
                WmsStockHistoryLast,
                sa.and_(
                    WmsStockHistoryLast.nomenclature_uuid == WmsStockHistory.nomenclature_uuid,
                    WmsStockHistoryLast.ic_uuid == WmsStockHistory.ic_uuid,
                    WmsStockHistoryLast.period == to
                )
            ).filter(
                WmsStockHistory.ic_uuid.isnot(None),
                WmsStockHistory.nomenclature_uuid.isnot(None),
                WmsStockHistoryLast.nomenclature_uuid.is_(None),
                WmsStockHistory.period >= since,
                WmsStockHistory.period < to
            ).distinct().cte(name='q_nullified_stock')
        )

        # get future arrivals for zeroed-stock ICs
        cte_expected_arrivals = (
            sa.select(
                sa_sql.literal(to, type_=sa.TIMESTAMP).label('last_wms_stock_date'),
                sa.func.max(cte_nullified_stock.c.period).label('period'),
                cte_nullified_stock.c.nomenclature_uuid,
                cte_nullified_stock.c.article,
                cte_nullified_stock.c.ic_uuid,
                cte_nullified_stock.c.name,
                sa.func.coalesce(
                    sa.func.sum(FutureArrivalsStock.receipt_in_progress_qty),
                    0.0
                ).label('receipt_in_progress_qty')
            ).outerjoin(
                FutureArrivalsStock,
                sa.and_(
                    FutureArrivalsStock.nomenclature_uuid == cte_nullified_stock.c.nomenclature_uuid,
                    FutureArrivalsStock.ic_uuid == cte_nullified_stock.c.ic_uuid,
                    FutureArrivalsStock.warehouse_uuid == WAREHOUSE_OF_GOODS_UUID
                )
            ).group_by(
                cte_nullified_stock.c.nomenclature_uuid,
                cte_nullified_stock.c.article,
                cte_nullified_stock.c.ic_uuid,
                cte_nullified_stock.c.name
            ).cte(name='q_expected_arrivals')
        )

        stmt = sa.select(cte_expected_arrivals)
        return self.session.execute(stmt).all() if not return_stmt else stmt

    def update_zeroed_stock_history(self, zeroed_stock_stmt: sa_sql.Select, *, commit: bool = False)\
            -> list[sa_engine.LegacyRow]:
        stmt = (
            sa_pg.insert(ZeroedStockHistory)
            .from_select(
                [ZeroedStockHistory.zeroed_before.name,
                 ZeroedStockHistory.last_seen_at.name,
                 ZeroedStockHistory.nomenclature_uuid.name,
                 ZeroedStockHistory.article.name,
                 ZeroedStockHistory.ic_uuid.name,
                 ZeroedStockHistory.ic.name,
                 ZeroedStockHistory.receipt_in_progress_qty.name],
                zeroed_stock_stmt
            ).on_conflict_do_nothing(
                constraint=ZeroedStockHistory.UQ_CONSTR_NAME
            ).returning(ZeroedStockHistory.id)
        )
        result = self.session.execute(stmt).all()
        if commit:
            self.session.commit()
        return result

    def get_zeroed_stock_history_last_zeroed_date(self) -> datetime:
        stmt = sa.select(sa.func.max(ZeroedStockHistory.zeroed_before))
        return self.session.execute(stmt).scalar()

    def get_zeroed_stock_history(self, zeroed_before: datetime, with_receipts: bool = True)\
            -> list[ZeroedStockHistory]:
        stmt = (
            sa.select(ZeroedStockHistory)
            .filter(
                sa.and_(
                    ZeroedStockHistory.zeroed_before == zeroed_before,
                    True if with_receipts else ZeroedStockHistory.receipt_in_progress_qty == 0
                )
            )
        )
        return self.session.execute(stmt).scalars().all()

    def get_successor_ics(self, ic_uuids: list[sa_pg.UUID], lifecycle_statuses: list[str] = None,
                          *, return_stmt: bool = False):
        IcSuccessor = sa_aliased(Ic)  #type: Ic
        stmt = (
            sa.select(
                Ic.uuid.label(QuerySuccessorIcMap.IC_UUID),
                Ic.name.label(QuerySuccessorIcMap.IC),
                Ic.lifecycle_status.label(QuerySuccessorIcMap.IC_LIFECYCLE_STATUS),
                Ic.priority.label(QuerySuccessorIcMap.IC_PRIORITY),
                Nomenclature.uuid.label(QuerySuccessorIcMap.NOMENCLATURE_UUID),
                Nomenclature.article.label(QuerySuccessorIcMap.ARTICLE),
                IcSuccessor.uuid.label(QuerySuccessorIcMap.SUCCESSOR_IC_UUID),
                IcSuccessor.name.label(QuerySuccessorIcMap.SUCCESSOR_IC),
                IcSuccessor.lifecycle_status.label(QuerySuccessorIcMap.SUCCESSOR_IC_LIFECYCLE_STATUS),
                IcSuccessor.priority.label(QuerySuccessorIcMap.SUCCESSOR_IC_PRIORITY),
            ).outerjoin(
                Nomenclature,
                sa.and_(
                    Nomenclature.is_deleted == False,
                    Nomenclature.uuid == Ic.nomenclature_uuid,
                )
            ).outerjoin(
                IcSuccessor,
                sa.and_(
                    IcSuccessor.is_deleted == False,
                    IcSuccessor.nomenclature_uuid == Nomenclature.uuid,
                    IcSuccessor.priority > Ic.priority,
                    True if lifecycle_statuses is None else
                        sa.or_(
                            IcSuccessor.lifecycle_status.in_(lifecycle_statuses),
                            IcSuccessor.lifecycle_status.is_(None) if None in lifecycle_statuses else False
                        )
                )
            ).filter(
                Ic.is_deleted == False,
                Ic.uuid.in_(ic_uuids)
            ).order_by(
                Ic.name.asc(),
                Nomenclature.article.asc(),
                IcSuccessor.priority.asc()
            )
        )
        return stmt if return_stmt else self.session.execute(stmt).all()
