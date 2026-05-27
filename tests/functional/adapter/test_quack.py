"""Functional tests for the Quack environment.

These tests verify that dbt materializations work correctly when
targeting a remote DuckDB server via the Quack protocol.

Run with: pytest tests/functional/adapter/test_quack.py --profile quack

Note: Quack beta only supports the 'main' schema reliably. Custom schemas
have bugs with qualified name resolution and DROP SCHEMA. All tests
force schema='main' via profiles_config_update.
"""
import pytest
from dbt.tests.util import run_dbt


# --- Models ---

quack_table_model = """
{{ config(materialized='table') }}
SELECT 1 as id, 'hello' as name
"""

quack_view_model = """
{{ config(materialized='view') }}
SELECT * FROM {{ ref('quack_table') }}
"""

quack_incremental_model = """
{{ config(materialized='incremental') }}

{% if is_incremental() %}
SELECT 2 as id, 'second' as status
{% else %}
SELECT 1 as id, 'first' as status
{% endif %}
"""

quack_seed_csv = """id,name,value
1,alice,100
2,bob,200
3,charlie,300
"""


class QuackTestBase:
    """Base class for Quack tests. Forces schema=main because Quack beta
    does not support custom schemas with qualified name resolution."""

    @pytest.fixture(scope="class")
    def unique_schema(self):
        """Override dbt's unique_schema to always use 'main' for quack."""
        return "main"


# --- Test Classes ---


@pytest.mark.requires_quack
@pytest.mark.skip_profile("memory", "file", "buenavista", "md")
class TestQuackTableAndView(QuackTestBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "quack_table.sql": quack_table_model,
            "quack_view.sql": quack_view_model,
        }

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        # Verify table contents
        result = project.run_sql("SELECT * FROM quack_table", fetch="one")
        assert result[0] == 1
        assert result[1] == "hello"

        # Verify view reads from table
        result = project.run_sql("SELECT * FROM quack_view", fetch="one")
        assert result[0] == 1
        assert result[1] == "hello"

        # Re-run should succeed (idempotent)
        results = run_dbt(["run"])
        assert len(results) == 2


@pytest.mark.requires_quack
@pytest.mark.skip_profile("memory", "file", "buenavista", "md")
class TestQuackIncremental(QuackTestBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "quack_incremental.sql": quack_incremental_model,
        }

    def test_incremental(self, project):
        # First run creates the table with 1 row
        results = run_dbt(["run"])
        assert len(results) == 1
        count = project.run_sql(
            "SELECT COUNT(*) FROM quack_incremental", fetch="one"
        )[0]
        assert count == 1

        # Second run is incremental — adds 1 more row
        results = run_dbt(["run"])
        assert len(results) == 1
        count = project.run_sql(
            "SELECT COUNT(*) FROM quack_incremental", fetch="one"
        )[0]
        assert count == 2


@pytest.mark.requires_quack
@pytest.mark.skip_profile("memory", "file", "buenavista", "md")
class TestQuackSeed(QuackTestBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"quack_seed.csv": quack_seed_csv}

    def test_seed(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        count = project.run_sql(
            "SELECT COUNT(*) FROM quack_seed", fetch="one"
        )[0]
        assert count == 3

        # Verify data
        result = project.run_sql(
            "SELECT name FROM quack_seed WHERE id = 2", fetch="one"
        )
        assert result[0] == "bob"


@pytest.mark.requires_quack
@pytest.mark.skip_profile("memory", "file", "buenavista", "md")
class TestQuackFullRefresh(QuackTestBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "quack_incremental.sql": quack_incremental_model,
        }

    def test_full_refresh(self, project):
        # First run
        run_dbt(["run"])
        # Second run (incremental)
        run_dbt(["run"])
        count = project.run_sql(
            "SELECT COUNT(*) FROM quack_incremental", fetch="one"
        )[0]
        assert count == 2

        # Full refresh should reset to 1 row
        run_dbt(["run", "--full-refresh"])
        count = project.run_sql(
            "SELECT COUNT(*) FROM quack_incremental", fetch="one"
        )[0]
        assert count == 1
