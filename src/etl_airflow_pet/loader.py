from __future__ import annotations

from typing import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import Schema
from .incremental import Row, column_names
from .sql_utils import identifier, mssql_type, qualified_identifier, sql_literal, staging_table_name


def build_create_staging_sql(staging_table: str, schema: Schema) -> str:
    columns = ",\n    ".join(
        f"{identifier(name)} {mssql_type(type_name)}" for name, type_name in schema
    )
    return f"CREATE TABLE {staging_table} (\n    {columns}\n);"


def build_insert_staging_sql(staging_table: str, schema: Schema, rows: Sequence[Row]) -> str:
    columns = column_names(schema)
    column_list = ", ".join(columns)
    statements: list[str] = []

    for start in range(0, len(rows), 1000):
        chunk = rows[start : start + 1000]
        values = []
        for row in chunk:
            values.append("(" + ", ".join(sql_literal(row.get(col)) for col in columns) + ")")
        statements.append(
            f"INSERT INTO {staging_table} ({column_list})\nVALUES\n" + ",\n".join(values) + ";"
        )

    return "\n".join(statements)


def build_merge_sql(staging_table: str, dst_table: str, schema: Schema, key_field: str) -> str:
    dst = qualified_identifier(dst_table)
    key = identifier(key_field)
    columns = column_names(schema)
    update_columns = [col for col in columns if col != key]

    update_set = ",\n    ".join(f"target.{col} = source.{col}" for col in update_columns)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"source.{col}" for col in columns)

    return f"""
MERGE INTO {dst} AS target
USING {staging_table} AS source
    ON target.{key} = source.{key}
WHEN MATCHED THEN
    UPDATE SET
    {update_set}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_columns})
    VALUES ({insert_values});
""".strip()


def upsert_batch(engine: Engine, dst_table: str, schema: Schema, rows: Sequence[Row], key_field: str) -> int:
    if not rows:
        return 0

    staging_table = staging_table_name(dst_table)
    sql = "\n".join(
        [
            f"IF OBJECT_ID('tempdb..{staging_table}') IS NOT NULL DROP TABLE {staging_table};",
            build_create_staging_sql(staging_table, schema),
            build_insert_staging_sql(staging_table, schema, rows),
            build_merge_sql(staging_table, dst_table, schema, key_field),
        ]
    )

    with engine.begin() as conn:
        conn.execute(text(sql))

    return len(rows)