from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

pg_hook = PostgresHook('pg_prod')
CONN_URI = pg_hook.sqlalchemy_url

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    CONN_URI
)

Base = declarative_base()

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
