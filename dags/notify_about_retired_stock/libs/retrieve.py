import pandas

from db_model.db_broker import DbBroker
from db_model.mapping import QuerySiblingIcMap
from db_model.onec_extract.constants import IcLifecycleStatus


def get_zeroed_stock_with_siblings() -> pandas.DataFrame | None:
    with DbBroker() as db_broker:
        prev_check_date = db_broker.get_zeroed_stock_history_last_zeroed_date()
        # detect zero stock from 1C
        wms_zeroed_stock_stmt = db_broker.get_wms_zeroed_stock(since=prev_check_date, return_stmt=True)
        # update zero stock history table by info from 1C
        new_ids = db_broker.update_zeroed_stock_history(wms_zeroed_stock_stmt, commit=True)

        if not new_ids:
            return None

        # get all ICs with zero stock and no expected receipts
        last_check_date = db_broker.get_zeroed_stock_history_last_zeroed_date()
        result_nullified_stock = db_broker.get_zeroed_stock_history(last_check_date, with_receipts=False)

        # get siblings with active lifecycle statuses for zero-stock ICs
        ic_uuids = list({r.ic_uuid for r in result_nullified_stock})
        result_sibling_ics = db_broker.get_sibling_ics(
            ic_uuids=ic_uuids,
            lifecycle_statuses=IcLifecycleStatus.active_statuses() + [None],
            return_stmt=True
        )
        df = pandas.read_sql(result_sibling_ics, db_broker.session.connection())
        df = df[[QuerySiblingIcMap.IC,
                 QuerySiblingIcMap.IC_LIFECYCLE_STATUS,
                 QuerySiblingIcMap.ARTICLE,
                 QuerySiblingIcMap.SIBLING_IC,
                 QuerySiblingIcMap.SIBLING_IC_LIFECYCLE_STATUS]]
    return df
