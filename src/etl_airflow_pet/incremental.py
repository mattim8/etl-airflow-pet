from __future__ import annotations

from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import Schema
from .sql_utils import identifier, qualified_identifier, sql_literal

Row = dict[str, Any]


def column_names(schema: Schema) -> list[str]:
    return [identifier(name) for name, _ in schema]


def build_incremental_query(
    src_table: str,
    schema: Schema,
    watermark_field: str,
    batch_size: int,
    last_watermark: Any | None = None,
    last_key: Any | None = None,
    key_field: str = "event_id",
) -> str:
    table = qualified_identifier(src_table)
    watermark = identifier(watermark_field)
    key = identifier(key_field)
    columns = ", ".join(column_names(schema))

    where_clause = ""
    if last_watermark is not None:
        if last_key is None:
            where_clause = f"WHERE {watermark} > {sql_literal(last_watermark)}"
        else:
            where_clause = (
                f"WHERE ({watermark} > {sql_literal(last_watermark)} "
                f"OR ({watermark} = {sql_literal(last_watermark)} "
                f"AND {key} > {sql_literal(last_key)}))"
            )

    return (
        f"SELECT {columns}\n"
        f"FROM {table}\n"
        f"{where_clause}\n"
        f"ORDER BY {watermark}, {key}\n"
        f"LIMIT {int(batch_size)}"
    )


def extract_incremental(engine: Engine, query: str) -> list[Row]:
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row._mapping) for row in result]


def max_cursor(rows: Sequence[Row], watermark_field: str, key_field: str) -> tuple[Any, Any] | tuple[None, None]:
    if not rows:
        return None, None
    watermark = identifier(watermark_field)
    key = identifier(key_field)
    row = max(rows, key=lambda item: (item[watermark], item[key]))
    return row[watermark], row[key]