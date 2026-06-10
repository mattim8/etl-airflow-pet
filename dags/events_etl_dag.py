from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from etl_airflow_pet.config import EtlConfig
from etl_airflow_pet.db import destination_engine, source_engine
from etl_airflow_pet.quality import check_destination_quality
from etl_airflow_pet.runner import run_etl_once


CONFIG = EtlConfig()
DEFAULT_CURSOR = {"last_watermark": None, "last_key": None}


def _load_cursor() -> dict[str, Any]:
    raw_value = Variable.get(CONFIG.cursor_variable, default_var=json.dumps(DEFAULT_CURSOR))
    value = json.loads(raw_value)
    return {"last_watermark": value.get("last_watermark"), "last_key": value.get("last_key")}


def _save_cursor(last_watermark: Any, last_key: Any) -> None:
    Variable.set(
        CONFIG.cursor_variable,
        json.dumps(
            {
                "last_watermark": str(last_watermark) if last_watermark is not None else None,
                "last_key": last_key,
            }
        ),
    )


def load_events_batch(**context: Any) -> None:
    cursor = _load_cursor()
    logging.info("Starting ETL batch from cursor=%s", cursor)

    new_watermark, new_key, row_count = run_etl_once(
        source=source_engine(),
        destination=destination_engine(),
        config=CONFIG,
        last_watermark=cursor["last_watermark"],
        last_key=cursor["last_key"],
    )

    logging.info(
        "Finished ETL batch: rows=%s, old_cursor=%s, new_cursor=%s",
        row_count,
        cursor,
        {"last_watermark": new_watermark, "last_key": new_key},
    )
    context["ti"].xcom_push(key="row_count", value=row_count)
    context["ti"].xcom_push(
        key="cursor",
        value={
            "last_watermark": str(new_watermark) if new_watermark is not None else None,
            "last_key": new_key,
        },
    )


def run_quality_checks(**context: Any) -> None:
    check_destination_quality(
        engine=destination_engine(),
        dst_table=CONFIG.dst_table,
        watermark_field=CONFIG.watermark_field,
        key_field=CONFIG.key_field,
    )
    row_count = context["ti"].xcom_pull(task_ids="load_events_batch", key="row_count")
    logging.info("Destination quality checks passed. Loaded rows in this run: %s", row_count)


def save_cursor(**context: Any) -> None:
    cursor = context["ti"].xcom_pull(task_ids="load_events_batch", key="cursor")
    if not cursor:
        raise ValueError("Cursor was not produced by load_events_batch")
    _save_cursor(cursor["last_watermark"], cursor["last_key"])
    logging.info("Saved cursor after successful quality checks: %s", cursor)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 12,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=2),
}


with DAG(
    dag_id="postgres_to_mssql_events",
    description="Incremental idempotent load from PostgreSQL src_events to MS SQL dst_events.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/20 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "mssql"],
) as dag:
    load_task = PythonOperator(task_id="load_events_batch", python_callable=load_events_batch)
    quality_task = PythonOperator(task_id="check_destination_quality", python_callable=run_quality_checks)
    save_cursor_task = PythonOperator(task_id="save_cursor", python_callable=save_cursor)

    load_task >> quality_task >> save_cursor_task