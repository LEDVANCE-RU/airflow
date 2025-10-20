import uuid

from sqlalchemy import create_engine, TypeDecorator, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker, declarative_base
import os

ONEC_EXTRACT_SCHEMA = '1c_extract'
MAIN_SCHEMA = 'public'

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    ""
)

Base = declarative_base()

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    Base.metadata.create_all(bind=engine)
