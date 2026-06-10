from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Engine

from .config import EtlConfig
from .incremental import build_incremental_query, extract_incremental, max_cursor
from .loader import upsert_batch


def run_etl_once(
    source: Engine,
    destination: Engine,
    config: EtlConfig,
    last_watermark: Any | None,
    last_key: Any | None,
) -> tuple[Any | None, Any | None, int]:
    query = build_incremental_query(
        src_table=config.src_table,
        schema=config.schema,
        watermark_field=config.watermark_field,
        batch_size=config.batch_size,
        last_watermark=last_watermark,
        last_key=last_key,
        key_field=config.key_field,
    )
    rows = extract_incremental(source, query)
    if not rows:
        return last_watermark, last_key, 0

    upsert_batch(destination, config.dst_table, config.schema, rows, config.key_field)
    new_watermark, new_key = max_cursor(rows, config.watermark_field, config.key_field)
    return new_watermark, new_key, len(rows)