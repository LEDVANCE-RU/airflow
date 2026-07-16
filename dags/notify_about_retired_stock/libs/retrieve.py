from datetime import datetime

import pandas

from db_model.db_broker import DbBroker
from db_model.mapping import QuerySuccessorIcMap
from db_model.onec_extract.constants import IcLifecycleStatus
from notify_about_retired_stock.libs.constants import LAST_CHECK_KEY, DEFAULT_STOCK_HISTORY_HORIZON, \
    ARRIVAL_DOC_OBSOLESCENCE_THRESHOLD


def get_zeroed_stock_with_successors() -> pandas.DataFrame | None:
    def _set_last_check_date(db_broker: DbBroker, dt: datetime):
        db_broker.set_runtime_state(LAST_CHECK_KEY, dt)
        db_broker.session.commit()

    with DbBroker() as db_broker:
        actual_stock_datetime = db_broker.get_actual_stock_datetime()
        last_check = db_broker.get_runtime_state(LAST_CHECK_KEY)
        last_checked_at = last_check.value_ts if last_check else actual_stock_datetime - DEFAULT_STOCK_HISTORY_HORIZON
        zeroed_stock_stmt = db_broker.get_zeroed_stock(since=last_checked_at, to=actual_stock_datetime,
                                                       arrival_doc_obsolescence=ARRIVAL_DOC_OBSOLESCENCE_THRESHOLD,
                                                       return_stmt=True)
        # update zero stock history table by info from 1C
        new_ids = db_broker.update_zeroed_stock_history(zeroed_stock_stmt)

        if not new_ids:
            _set_last_check_date(db_broker, actual_stock_datetime)
            return None

        # get all ICs with zero stock and no expected receipts
        result_zeroed_stock = db_broker.get_zeroed_stock_history(actual_stock_datetime, with_expected_arrivals=False)

        # get siblings with active lifecycle statuses for zero-stock ICs (ignoring empty ICs)
        ic_uuids = list({r.ic_uuid for r in result_zeroed_stock if r.ic_uuid is not None})
        result_sibling_ics = db_broker.get_successor_ics(
            ic_uuids=ic_uuids,
            lifecycle_statuses=IcLifecycleStatus.active_and_undef_statuses(),
            return_stmt=True
        )
        df = pandas.read_sql(result_sibling_ics, db_broker.session.connection())
        df = df[[QuerySuccessorIcMap.IC,
                 QuerySuccessorIcMap.IC_LIFECYCLE_STATUS,
                 QuerySuccessorIcMap.IC_PRIORITY,
                 QuerySuccessorIcMap.ARTICLE,
                 QuerySuccessorIcMap.SUCCESSOR_IC,
                 QuerySuccessorIcMap.SUCCESSOR_IC_LIFECYCLE_STATUS,
                 QuerySuccessorIcMap.SUCCESSOR_IC_PRIORITY]]

        _set_last_check_date(db_broker, actual_stock_datetime)

    return df
