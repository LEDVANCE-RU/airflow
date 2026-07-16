import datetime
import uuid
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import TypeDecorator, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Dialect


class DateTimeMSK(TypeDecorator[datetime.datetime]):
    """Timezone Aware DateTime.

    Ensure MSK is stored in the database and that TZ aware dates are returned for all dialects.
    """
    _TZ_MSK = ZoneInfo("Europe/Moscow")

    impl = DateTime(timezone=True)
    cache_ok = True

    @property
    def python_type(self) -> type[datetime.datetime]:
        return datetime.datetime

    def process_bind_param(self, value: Optional[datetime.datetime], dialect: Dialect) -> Optional[datetime.datetime]:
        if value is None:
            return value
        return value.astimezone(self._TZ_MSK).replace(tzinfo=None)

    def process_result_value(self, value: Optional[datetime.datetime], dialect: Dialect) -> Optional[datetime.datetime]:
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=self._TZ_MSK)
        return value


class NullableUUID(TypeDecorator):
    impl = UUID
    cache_ok = True
    NULL_UUID = uuid.UUID('00000000-0000-0000-0000-000000000000')

    class comparator_factory(UUID.Comparator):
        def is_(self, other):
            if other is None:
                return self == self.type.NULL_UUID
            return super().is_(other)

        def is_not(self, other):
            if other is None:
                return self != self.type.NULL_UUID
            return super().is_not(other)

    def process_bind_param(self, value, dialect):
        if value is None:
            return self.NULL_UUID
        return value

    def process_result_value(self, value, dialect):
        if value == self.NULL_UUID:
            return None
        return value


class NullableUUIDString(TypeDecorator):
    impl = String(36)
    cache_ok = True
    NULL_UUID_STR = '00000000-0000-0000-0000-000000000000'

    def process_bind_param(self, value, dialect):
        if value is None:
            return self.NULL_UUID_STR
        return str(value) if value else self.NULL_UUID_STR

    def process_result_value(self, value, dialect):
        if value == self.NULL_UUID_STR:
            return None
        try:
            return uuid.UUID(value)
        except ValueError:
            return None
