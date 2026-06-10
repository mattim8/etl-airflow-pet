from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .config import mssql_url, postgres_url


def source_engine() -> Engine:
    return create_engine(postgres_url(), pool_pre_ping=True)


def destination_engine() -> Engine:
    return create_engine(mssql_url(), pool_pre_ping=True, fast_executemany=True)