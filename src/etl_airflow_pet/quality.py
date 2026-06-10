from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .sql_utils import identifier, qualified_identifier


def check_destination_quality(engine: Engine, dst_table: str, watermark_field: str, key_field: str) -> None:
    table = qualified_identifier(dst_table)
    key = identifier(key_field)
    watermark = identifier(watermark_field)

    duplicate_sql = text(
        f"""
        SELECT {key}, COUNT(*) AS cnt
        FROM {table}
        GROUP BY {key}
        HAVING COUNT(*) > 1
        """
    )
    null_sql = text(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {table}
        WHERE {key} IS NULL OR {watermark} IS NULL
        """
    )

    with engine.connect() as conn:
        duplicates = [dict(row._mapping) for row in conn.execute(duplicate_sql)]
        if duplicates:
            raise ValueError(f"Found duplicate {key} values: {duplicates[:10]}")

        null_count = conn.execute(null_sql).scalar_one()
        if null_count:
            raise ValueError(f"Found rows with NULL {key} or {watermark}: {null_count}")