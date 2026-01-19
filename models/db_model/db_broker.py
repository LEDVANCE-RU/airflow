from datetime import timedelta, datetime
from functools import wraps
from typing import Callable, Any

import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
import sqlalchemy.dialects.postgresql as sa_pg
import sqlalchemy.sql as sa_sql
from sqlalchemy.engine import Connection
from sqlalchemy.orm import aliased as sa_aliased

from constants import TZ_MSK, TZ_UTC
from db_model.main import SessionLocal
from db_model.mapping import QuerySuccessorIcMap
from db_model.core.model import ZeroedStockHistory, LemanaProOrder, RuntimeState
from db_model.marketprovider.model import Category, Product, TempProduct
from db_model.onec_extract.constants import WarehouseUUID
from db_model.onec_extract.model import WmsStockHistory, Nomenclature, Ic, FutureArrivalsStock, StockHistory
from lemana_pro_orders_processing.libs.order_parser import Order


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

    def get_runtime_state(self, key: str) -> RuntimeState | None:
        stmt = sa.select(RuntimeState).filter(RuntimeState.key == key)
        return self.session.execute(stmt).scalar_one_or_none()

    def set_runtime_state(self, key: str, value: Any, commit: bool = False):
        value_col = RuntimeState.value_str
        if isinstance(value, datetime):
            value_col = RuntimeState.value_ts
        values = {
            RuntimeState.key.key: key,
            value_col.key: value,
            RuntimeState.updated_on.key: datetime.now(TZ_UTC)
        }
        stmt = (
            sa_pg.insert(RuntimeState)
            .values(values)
            .on_conflict_do_update(
                index_elements=[RuntimeState.key],
                set_=values
            )
        )
        self.session.execute(stmt)
        if commit:
            self.session.commit()

    # def get_last_wms_stock_date(self) -> datetime:
    #     return self.session.query(sa.func.max(WmsStockHistory.period)).scalar()
    #
    # def get_wms_zeroed_stock(self, since: datetime = None, to: datetime = None,
    #                          *, return_stmt: bool = False) -> list[sa.engine.Row] | sa_sql.Select:
    #     """Get ICs with zeroed stock since given datetime + planned arrivals."""
    #
    #     # actual stock datetime
    #     if not to:
    #         to = self.get_last_wms_stock_date()
    #     since = since or to - timedelta(days=30)
    #
    #     WmsStockHistoryLast = sa_aliased(WmsStockHistory)  # type: WmsStockHistory
    #
    #     # get zeroed-stock ICs since given datetime
    #     cte_nullified_stock = (
    #         sa.select(
    #             Nomenclature.uuid.label('nomenclature_uuid'),
    #             Nomenclature.article,
    #             Ic.uuid.label('ic_uuid'),
    #             Ic.name,
    #             WmsStockHistory.period
    #         ).select_from(
    #             WmsStockHistory
    #         ).join(
    #             Nomenclature,
    #             sa.and_(
    #                 Nomenclature.is_deleted == False,
    #                 sa.cast(Nomenclature.uuid, sa.String) == WmsStockHistory.nomenclature_uuid,
    #             )
    #         ).outerjoin(
    #             Ic,
    #             sa.and_(
    #                 Ic.is_deleted == False,
    #                 Ic.nomenclature_uuid == Nomenclature.uuid,
    #                 Ic.uuid == WmsStockHistory.ic_uuid
    #             )
    #         ).outerjoin(
    #             WmsStockHistoryLast,
    #             sa.and_(
    #                 WmsStockHistoryLast.nomenclature_uuid == WmsStockHistory.nomenclature_uuid,
    #                 WmsStockHistoryLast.ic_uuid == WmsStockHistory.ic_uuid,
    #                 WmsStockHistoryLast.period == to
    #             )
    #         ).filter(
    #             WmsStockHistory.ic_uuid.isnot(None),
    #             WmsStockHistory.nomenclature_uuid.isnot(None),
    #             WmsStockHistoryLast.nomenclature_uuid.is_(None),
    #             WmsStockHistory.period >= since,
    #             WmsStockHistory.period < to
    #         ).distinct().cte(name='q_nullified_stock')
    #     )
    #
    #     # get future arrivals for zeroed-stock ICs
    #     cte_expected_arrivals = (
    #         sa.select(
    #             sa_sql.literal(to, type_=sa.TIMESTAMP).label('last_wms_stock_date'),
    #             sa.func.max(cte_nullified_stock.c.period).label('period'),
    #             cte_nullified_stock.c.nomenclature_uuid,
    #             cte_nullified_stock.c.article,
    #             cte_nullified_stock.c.ic_uuid,
    #             cte_nullified_stock.c.name,
    #             sa.func.coalesce(
    #                 sa.func.sum(FutureArrivalsStock.receipt_in_progress_qty),
    #                 0.0
    #             ).label('receipt_in_progress_qty')
    #         ).outerjoin(
    #             FutureArrivalsStock,
    #             sa.and_(
    #                 FutureArrivalsStock.nomenclature_uuid == cte_nullified_stock.c.nomenclature_uuid,
    #                 FutureArrivalsStock.ic_uuid == cte_nullified_stock.c.ic_uuid,
    #                 FutureArrivalsStock.warehouse_uuid == WarehouseUUID.GOODS
    #             )
    #         ).group_by(
    #             cte_nullified_stock.c.nomenclature_uuid,
    #             cte_nullified_stock.c.article,
    #             cte_nullified_stock.c.ic_uuid,
    #             cte_nullified_stock.c.name
    #         ).cte(name='q_expected_arrivals')
    #     )
    #
    #     stmt = sa.select(cte_expected_arrivals)
    #     return self.session.execute(stmt).all() if not return_stmt else stmt

    def get_actual_stock_datetime(self) -> datetime:
        now = datetime.now(TZ_MSK).replace(tzinfo=None)
        stmt = (
            sa.select(sa.func.max(StockHistory.stock_date))
            .filter(StockHistory.stock_date <= now + timedelta(days=1))
        )
        return self.session.execute(stmt).scalar()

    def get_zeroed_stock(self, since: datetime, to: datetime, arrival_doc_obsolescence: timedelta = None,
                         *, return_stmt: bool = False) -> list[sa.engine.Row] | sa_sql.Select:
        def _build_cte_stock(extra_filter, name: str):
            return (
                sa.select(
                    StockHistory.nomenclature_uuid,
                    StockHistory.ic_uuid,
                    sa.func.max(StockHistory.stock_date).label(StockHistory.stock_date.key)
                ).filter(
                    extra_filter,
                    StockHistory.warehouse_uuid.in_(wh_uuids)
                ).group_by(
                    StockHistory.nomenclature_uuid,
                    StockHistory.ic_uuid
                ).having(
                    sa.func.sum(StockHistory.stock) > 0
                ).cte(name)
            )

        wh_uuids = [WarehouseUUID.GOODS,
                    WarehouseUUID.BLOCK,
                    WarehouseUUID.SHORTAGES]

        now_msk_naive = datetime.now(TZ_MSK).replace(tzinfo=None)

        cte_last_stock = _build_cte_stock(StockHistory.stock_date == to, 'q_last_stock')
        cte_prev_stock = _build_cte_stock(
            sa.and_(
                StockHistory.stock_date >= since,
                StockHistory.stock_date < to
            ),
            'q_prev_stock'
        )
        cte_zeroed_stock = (
            sa.select(
                cte_prev_stock.c.nomenclature_uuid,
                cte_prev_stock.c.ic_uuid,
                cte_prev_stock.c.stock_date
            ).outerjoin(
                cte_last_stock,
                sa.and_(
                    cte_last_stock.c.nomenclature_uuid == cte_prev_stock.c.nomenclature_uuid,
                    cte_last_stock.c.ic_uuid == cte_prev_stock.c.ic_uuid,
                )
            ).filter(
                cte_last_stock.c.nomenclature_uuid.is_(None)
            ).cte('q_zeroed_stock')
        )

        min_arrival_doc_date = None if not arrival_doc_obsolescence else now_msk_naive - arrival_doc_obsolescence

        cte_mixin_arrivals = (
            sa.select(
                sa_sql.literal(to, type_=sa.TIMESTAMP).label('zeroed_before'),
                cte_zeroed_stock.c.stock_date,
                cte_zeroed_stock.c.nomenclature_uuid,
                Nomenclature.article,
                cte_zeroed_stock.c.ic_uuid,
                Ic.name,
                sa.func.coalesce(
                    sa.func.sum(FutureArrivalsStock.yet_to_arrive_qty),
                    0.0
                ).label(FutureArrivalsStock.yet_to_arrive_qty.key),
                sa.func.coalesce(
                    sa.func.sum(FutureArrivalsStock.receipt_in_progress_qty),
                    0.0
                ).label(FutureArrivalsStock.receipt_in_progress_qty.key)
            ).join(
                Nomenclature,
                Nomenclature.uuid == cte_zeroed_stock.c.nomenclature_uuid,
            ).outerjoin(
                Ic,
                sa.and_(
                    Ic.nomenclature_uuid == cte_zeroed_stock.c.nomenclature_uuid,
                    Ic.uuid == cte_zeroed_stock.c.ic_uuid
                )
            ).outerjoin(FutureArrivalsStock,
                sa.and_(
                    FutureArrivalsStock.nomenclature_uuid == cte_zeroed_stock.c.nomenclature_uuid,
                    FutureArrivalsStock.ic_uuid == cte_zeroed_stock.c.ic_uuid,
                    FutureArrivalsStock.warehouse_uuid.in_(wh_uuids),
                    True if not min_arrival_doc_date
                        else FutureArrivalsStock.document_date >= min_arrival_doc_date
                )
            ).group_by(
                cte_zeroed_stock.c.nomenclature_uuid,
                cte_zeroed_stock.c.ic_uuid,
                cte_zeroed_stock.c.stock_date,
                Nomenclature.article,
                Ic.name
            ).cte('q_arrivals')
        )
        stmt = sa.select(cte_mixin_arrivals)
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
                 ZeroedStockHistory.yet_to_arrive_qty.name,
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

    def get_zeroed_stock_history(self, zeroed_before: datetime, with_expected_arrivals: bool = True)\
            -> list[ZeroedStockHistory]:
        stmt = (
            sa.select(ZeroedStockHistory)
            .filter(
                sa.and_(
                    ZeroedStockHistory.zeroed_before == zeroed_before,
                    True if with_expected_arrivals else
                        sa.and_(
                            ZeroedStockHistory.receipt_in_progress_qty == 0,
                            ZeroedStockHistory.yet_to_arrive_qty == 0
                        )
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

    def insert_lemana_pro_order(self, order: Order):
        values = {
            LemanaProOrder.order_num.key: order.num,
            LemanaProOrder.received_at.key: order.received_at,
            LemanaProOrder.contents_json.key: order.to_dict()
        }
        stmt = sa_pg.insert(LemanaProOrder).values(values)
        self.session.execute(stmt)
        self.session.commit()

    def upsert_marketprovider_categories(self, categories: list[Category]):
        if not categories:
            return
        values = [c.to_dict() for c in categories]
        insert_stmt = sa_pg.insert(Category).values(values)
        index_elements = [Category.id]
        stmt = (
            insert_stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_=Category.get_update_set_for_upsert(insert_stmt, index_elements)
            )
        )
        self.session.execute(stmt)
        self.session.commit()

    def create_temp_products_table(self, conn: Connection):
        TempProduct.__table__.create(conn)

    def upload_marketprovider_temp_products(self, conn: Connection, products: list[TempProduct]):
        values = [p.to_dict() for p in products]
        stmt = sa_pg.insert(TempProduct).values(values)
        conn.execute(stmt)

    def upsert_marketprovider_products_from_temp_table(self, conn: Connection):
        # mark main image as downloaded in product temp table in case URL did not change
        # and image has been already downloaded according to product persistent table
        stmt_main_image = (
            sa.select(TempProduct.id)
            .join(Product, Product.id == TempProduct.id)
            .filter(
                sa.and_(
                    TempProduct.main_image_url == Product.main_image_url,
                    Product.main_image_downloaded == True
                )
            ).subquery()
        )
        stmt_main_image_update = (
            sa.update(TempProduct)
            .where(TempProduct.id.in_(sa.select(stmt_main_image)))
            .values({TempProduct.main_image_downloaded.key: True})
        )
        conn.execute(stmt_main_image_update)

        # upsert products from temp table
        cols = [c.name for c in Product.__table__.columns]
        insert_stmt = sa_pg.insert(Product).from_select(cols, sa.select(TempProduct))
        index_elements = [Product.id]
        stmt = (
            insert_stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_=Product.get_update_set_for_upsert(insert_stmt, index_elements)
            )
        )
        conn.execute(stmt)
        conn.connection.commit()

    def get_marketprovider_product_files_to_download(self):
        stmt = (
            sa.select(Product.id, Product.main_image_url)
            .filter(
                Product.main_image_downloaded == False,
                Product.main_image_url.isnot(None)
            )
        )
        return self.session.execute(stmt).all()

    def update_marketprovider_product_main_image_relpath(self, id_: int, relpath: str):
        stmt = (
            sa.update(Product)
            .filter(Product.id == id_)
            .values({
                Product.main_image_relpath: relpath,
                Product.main_image_downloaded: True
            })
        )
        self.session.execute(stmt)
        self.session.commit()
