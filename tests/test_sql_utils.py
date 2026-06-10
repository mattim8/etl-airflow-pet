import pytest

from etl_airflow_pet.sql_utils import identifier, qualified_identifier, staging_table_name


def test_identifier_rejects_unsafe_names():
    with pytest.raises(ValueError):
        identifier("dst_events; drop table users")


def test_qualified_identifier_accepts_schema_table():
    assert qualified_identifier("dbo.dst_events") == "dbo.dst_events"


def test_staging_table_name_flattens_schema_name():
    assert staging_table_name("dbo.dst_events") == "#stg_dbo_dst_events"