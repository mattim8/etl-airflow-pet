from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Sequence


Schema = Sequence[tuple[str, str]]


@dataclass(frozen=True)
class EtlConfig:
    src_table: str = os.getenv("SRC_TABLE", "src_events")
    dst_table: str = os.getenv("DST_TABLE", "dbo.dst_events")
    schema: Schema = (
        ("event_id", "bigint"),
        ("event_ts", "timestamp"),
        ("payload", "text"),
        ("updated_ts", "timestamp"),
    )
    watermark_field: str = os.getenv("WATERMARK_FIELD", "updated_ts")
    key_field: str = os.getenv("KEY_FIELD", "event_id")
    batch_size: int = int(os.getenv("BATCH_SIZE", "10000"))
    cursor_variable: str = os.getenv("CURSOR_VARIABLE", "src_events_to_dst_events_cursor")


def postgres_url() -> str:
    return (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'etl_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'etl_password')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'events')}"
    )


def mssql_url() -> str:
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASSWORD", "YourStrong!Passw0rd")
    host = os.getenv("MSSQL_HOST", "localhost")
    port = os.getenv("MSSQL_PORT", "1433")
    database = os.getenv("MSSQL_DB", "events_dw")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    return (
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}"
        f"?driver={driver}&TrustServerCertificate=yes"
    )