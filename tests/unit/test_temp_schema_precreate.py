"""Regression tests for dbt-duckdb #882: concurrent CREATE SCHEMA IF NOT
EXISTS on the dbt_temp schema in an attached target catalog (e.g. Quack)
fails with a write-write conflict.

Environment.initialize_db pre-creates the temp schema in the target
database once per run, so the per-model CREATE SCHEMA is a no-op.
"""

import duckdb

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment


def _schema_exists(conn, catalog, schema):
    rows = conn.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE catalog_name = ? AND schema_name = ?",
        [catalog, schema],
    ).fetchall()
    return len(rows) == 1


def test_initialize_db_precreates_temp_schema_in_attached_target(tmp_path):
    """The temp schema is created in the attached target database, not only
    for MotherDuck."""
    attached_path = str(tmp_path / "catalog.duckdb")
    duckdb.connect(attached_path).close()

    creds = DuckDBCredentials(
        path=":memory:",
        database="catalog",
        attach=[Attachment(path=attached_path, alias="catalog")],
    )
    conn = Environment.initialize_db(creds)
    try:
        assert _schema_exists(conn, "catalog", "dbt_temp")
        # The primary database is untouched: the schema is only pre-created
        # in the target.
        assert not _schema_exists(conn, "memory", "dbt_temp")
    finally:
        conn.close()


def test_initialize_db_does_not_precreate_temp_schema_for_primary_target():
    """When the target is the primary database, initialize_db does not
    pre-create the temp schema (preserves the previous local behaviour)."""
    creds = DuckDBCredentials(path=":memory:", database="memory")
    conn = Environment.initialize_db(creds)
    try:
        assert not _schema_exists(conn, "memory", "dbt_temp")
    finally:
        conn.close()
