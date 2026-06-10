from etl_airflow_pet.config import EtlConfig
from etl_airflow_pet.incremental import build_incremental_query, max_cursor


def test_incremental_query_uses_composite_cursor():
    cfg = EtlConfig()
    sql = build_incremental_query(
        src_table=cfg.src_table,
        schema=cfg.schema,
        watermark_field=cfg.watermark_field,
        batch_size=100,
        last_watermark="2026-01-01 09:20:00",
        last_key=3,
        key_field=cfg.key_field,
    )

    assert "updated_ts > '2026-01-01 09:20:00'" in sql
    assert "updated_ts = '2026-01-01 09:20:00'" in sql
    assert "event_id > 3" in sql
    assert "ORDER BY updated_ts, event_id" in sql
    assert "LIMIT 100" in sql


def test_max_cursor_uses_watermark_and_key():
    rows = [
        {"event_id": 1, "updated_ts": "2026-01-01 09:00:00"},
        {"event_id": 3, "updated_ts": "2026-01-01 09:20:00"},
        {"event_id": 4, "updated_ts": "2026-01-01 09:20:00"},
    ]

    assert max_cursor(rows, "updated_ts", "event_id") == ("2026-01-01 09:20:00", 4)