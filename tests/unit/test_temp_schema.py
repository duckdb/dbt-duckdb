"""initialize_db creates the temp schema in an attached target database up front,
so concurrent models do not race to create it (#882)."""

import duckdb
import pytest

from dbt.adapters.duckdb.constants import DEFAULT_TEMP_SCHEMA_NAME
from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment


def temp_schema_catalogs(conn):
    rows = conn.execute(
        "SELECT catalog_name FROM information_schema.schemata WHERE schema_name = ?",
        [DEFAULT_TEMP_SCHEMA_NAME],
    ).fetchall()
    return {row[0] for row in rows}


@pytest.fixture
def attached_path(tmp_path):
    path = str(tmp_path / "warehouse.duckdb")
    duckdb.connect(path).close()
    return path


@pytest.mark.parametrize("database", ["Warehouse", "warehouse"])
def test_creates_temp_schema_in_attached_target(attached_path, database):
    creds = DuckDBCredentials(
        path=":memory:",
        database=database,
        attach=[Attachment(path=attached_path, alias="Warehouse")],
    )
    conn = Environment.initialize_db(creds)
    try:
        assert temp_schema_catalogs(conn) == {"Warehouse"}
    finally:
        conn.close()


def test_skips_primary_target():
    creds = DuckDBCredentials(path=":memory:", database="memory")
    conn = Environment.initialize_db(creds)
    try:
        assert temp_schema_catalogs(conn) == set()
    finally:
        conn.close()


def test_skips_target_not_attached(attached_path):
    creds = DuckDBCredentials(
        path=":memory:",
        database="elsewhere",
        attach=[Attachment(path=attached_path, alias="warehouse")],
    )
    conn = Environment.initialize_db(creds)
    try:
        assert temp_schema_catalogs(conn) == set()
    finally:
        conn.close()


def test_skips_read_only_target(attached_path):
    creds = DuckDBCredentials(
        path=":memory:",
        database="warehouse",
        attach=[Attachment(path=attached_path, alias="warehouse", read_only=True)],
    )
    conn = Environment.initialize_db(creds)
    try:
        assert temp_schema_catalogs(conn) == set()
    finally:
        conn.close()
