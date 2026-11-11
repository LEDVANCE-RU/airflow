from sqlalchemy import event
from sqlalchemy.orm import ORMExecuteState, with_loader_criteria

from db_model.onec_extract.model import AbstractBaseModel
from db_model.main import SessionLocal


@event.listens_for(SessionLocal, "do_orm_execute")
def _add_filtering_criteria(execute_state: ORMExecuteState):
    if execute_state.is_select:
        execute_state.statement = execute_state.statement.options(
            with_loader_criteria(
                AbstractBaseModel,
                lambda cls: cls.is_deleted == False if hasattr(cls, "is_deleted") else None,
                include_aliases=True,
            )
        )