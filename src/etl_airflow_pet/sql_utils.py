from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE_MAP_MSSQL = {
    "bigint": "BIGINT",
    "timestamp": "DATETIME2",
    "text": "NVARCHAR(MAX)",
}


def identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def qualified_identifier(name: str) -> str:
    parts = name.split(".")
    if not parts or any(not part for part in parts):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return ".".join(identifier(part) for part in parts)


def staging_table_name(dst_table: str) -> str:
    return "#stg_" + "_".join(identifier(part) for part in dst_table.split("."))


def mssql_type(type_name: str) -> str:
    try:
        return _TYPE_MAP_MSSQL[type_name.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported type: {type_name!r}") from exc


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, datetime):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, date):
        return "'" + value.isoformat() + "'"
    return "'" + str(value).replace("'", "''") + "'"