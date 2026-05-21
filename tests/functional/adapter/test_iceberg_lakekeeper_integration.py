"""Functional integration tests for Iceberg REST catalog (lakekeeper) with dbt-duckdb.

Tests are skipped automatically when lakekeeper is not reachable.

Configuration via environment variables:
  LAKEKEEPER_URL       Base URL of the lakekeeper server (default: http://localhost:8181)
  LAKEKEEPER_WAREHOUSE Iceberg warehouse name to use (default: demo)

The tests verify that the table materialization correctly separates CTAS and
ALTER...RENAME into different transactions, which is required by DuckDB's Iceberg
extension — CTAS and ALTER RENAME cannot share a transaction against a REST catalog.
"""
import os
import urllib.error
import urllib.request

import pytest
from dbt.tests.util import run_dbt

_DEFAULT_LAKEKEEPER_URL = "http://localhost:8181"
_DEFAULT_LAKEKEEPER_WAREHOUSE = "demo"


def _lakekeeper_available(url: str) -> bool:
    try:
        urllib.request.urlopen(f"{url}/management/v1/warehouse", timeout=3)
        return True
    except Exception:
        return False


models__simple_table_sql = """
{{ config(materialized='table', database='iceberg_catalog') }}
select 1 as id, 'hello' as msg
union all
select 2 as id, 'world' as msg
"""

models__incremental_sql = """
{{ config(
    materialized='incremental',
    database='iceberg_catalog',
    incremental_strategy='append',
) }}

{% if is_incremental() %}
select 3 as id, 'iceberg' as msg
{% else %}
select 1 as id, 'hello' as msg
union all
select 2 as id, 'world' as msg
{% endif %}
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestIcebergLakekeeperTableMaterialization:
    """Table materialization against a lakekeeper Iceberg REST catalog.

    Verifies that:
    1. First run: CTAS creates the intermediate table and rename moves it into place.
    2. Second run: the intermediate CTAS commits before the rename, satisfying the
       Iceberg constraint that CTAS and ALTER...RENAME cannot share a transaction.
    """

    @pytest.fixture(scope="class")
    def lakekeeper_url(self):
        url = os.environ.get("LAKEKEEPER_URL", _DEFAULT_LAKEKEEPER_URL)
        if not _lakekeeper_available(url):
            pytest.skip(f"lakekeeper not reachable at {url}")
        return url

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, lakekeeper_url, unique_schema):
        warehouse = os.environ.get("LAKEKEEPER_WAREHOUSE", _DEFAULT_LAKEKEEPER_WAREHOUSE)
        target = dict(dbt_profile_target)
        target["schema"] = unique_schema
        target["path"] = target.get("path", ":memory:")
        target["attach"] = [
            {
                "alias": "iceberg_catalog",
                "path": warehouse,
                "type": "iceberg",
                "options": {
                    "endpoint": f"{lakekeeper_url}/catalog",
                    "token": "",
                },
            }
        ]
        return {"test": {"outputs": {"dev": target}, "target": "dev"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_table.sql": models__simple_table_sql,
        }

    def test_first_run_creates_table(self, project):
        """First run: intermediate relation is created, then renamed to target."""
        result = run_dbt(["run", "--select", "simple_table"], expect_pass=True)
        assert len(result.results) == 1
        assert result.results[0].status.value == "success"

        row_count = project.run_sql(
            f"select count(*) from iceberg_catalog.{project.test_schema}.simple_table",
            fetch="one",
        )[0]
        assert row_count == 2

    def test_second_run_rename_in_separate_transaction(self, project):
        """Second run: CTAS commits, then rename opens a new transaction.

        This is the critical path — without skip_auto_begin for iceberg, DuckDB
        raises an error because CTAS and ALTER...RENAME share a transaction.
        """
        result = run_dbt(["run", "--select", "simple_table"], expect_pass=True)
        assert len(result.results) == 1
        assert result.results[0].status.value == "success"

        row_count = project.run_sql(
            f"select count(*) from iceberg_catalog.{project.test_schema}.simple_table",
            fetch="one",
        )[0]
        assert row_count == 2

    def test_full_refresh_rename_in_separate_transaction(self, project):
        """--full-refresh exercises the incremental → full-replace rename path."""
        result = run_dbt(
            ["run", "--select", "simple_table", "--full-refresh"],
            expect_pass=True,
        )
        assert len(result.results) == 1
        assert result.results[0].status.value == "success"

        row_count = project.run_sql(
            f"select count(*) from iceberg_catalog.{project.test_schema}.simple_table",
            fetch="one",
        )[0]
        assert row_count == 2


@pytest.mark.skip_profile("buenavista", "md")
class TestIcebergLakekeeperIncrementalMaterialization:
    """Incremental materialization against a lakekeeper Iceberg REST catalog."""

    @pytest.fixture(scope="class")
    def lakekeeper_url(self):
        url = os.environ.get("LAKEKEEPER_URL", _DEFAULT_LAKEKEEPER_URL)
        if not _lakekeeper_available(url):
            pytest.skip(f"lakekeeper not reachable at {url}")
        return url

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, lakekeeper_url, unique_schema):
        warehouse = os.environ.get("LAKEKEEPER_WAREHOUSE", _DEFAULT_LAKEKEEPER_WAREHOUSE)
        target = dict(dbt_profile_target)
        target["schema"] = unique_schema
        target["path"] = target.get("path", ":memory:")
        target["attach"] = [
            {
                "alias": "iceberg_catalog",
                "path": warehouse,
                "type": "iceberg",
                "options": {
                    "endpoint": f"{lakekeeper_url}/catalog",
                    "token": "",
                },
            }
        ]
        return {"test": {"outputs": {"dev": target}, "target": "dev"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": models__incremental_sql,
        }

    def test_incremental_first_run(self, project):
        result = run_dbt(["run", "--select", "incremental_model"], expect_pass=True)
        assert len(result.results) == 1
        row_count = project.run_sql(
            f"select count(*) from iceberg_catalog.{project.test_schema}.incremental_model",
            fetch="one",
        )[0]
        assert row_count == 2

    @pytest.mark.xfail(
        reason=(
            "DuckDB Iceberg: information_schema.columns returns '__' for Iceberg table "
            "columns, breaking get_columns_in_relation. Tracked upstream in duckdb/duckdb-iceberg."
        ),
        strict=False,
    )
    def test_incremental_second_run_appends(self, project):
        result = run_dbt(["run", "--select", "incremental_model"], expect_pass=True)
        assert len(result.results) == 1
        row_count = project.run_sql(
            f"select count(*) from iceberg_catalog.{project.test_schema}.incremental_model",
            fetch="one",
        )[0]
        assert row_count == 3

    def test_incremental_full_refresh(self, project):
        """Full-refresh incremental exercises the CTAS+rename transaction split."""
        result = run_dbt(
            ["run", "--select", "incremental_model", "--full-refresh"],
            expect_pass=True,
        )
        assert len(result.results) == 1
        row_count = project.run_sql(
            f"select count(*) from iceberg_catalog.{project.test_schema}.incremental_model",
            fetch="one",
        )[0]
        assert row_count == 2
