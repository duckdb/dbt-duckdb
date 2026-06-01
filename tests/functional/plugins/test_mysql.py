import pytest

from dbt.tests.util import run_dbt


@pytest.mark.skip(
    reason="""
Skipping by default because it requires a running MySQL server reachable
from the test process plus the duckdb 'mysql' extension installed/loaded.

Exercise locally with:
    pytest --profile=file tests/functional/plugins/test_mysql.py

This test guards #696: duckdb__create_schema must skip CREATE SCHEMA for
attached MySQL databases (type='mysql') rather than raising. Mirror of
test_postgres.py's @pytest.mark.skip shape so CI stays green while the
case remains exercisable locally."""
)
class TestMySQLPlugin:

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "attach": [
                            {
                                "path": "mysql://root:root@localhost:3306/mydb",
                                "type": "mysql",
                                "alias": "my_mysql",
                            }
                        ],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": """
                {{ config(materialized='table', database='my_mysql', schema='mydb') }}
                select 1 as id
            """,
        }

    def test_mysql_attached_skips_create_schema(self, project):
        # Before #696 this would fail because duckdb__create_schema attempted
        # CREATE SCHEMA against the attached MySQL database. After the fix
        # the create_schema call is skipped for attached MySQL databases
        # (postgres and other attached engines keep CREATE SCHEMA IF NOT EXISTS).
        #
        # Run once: exercises the initial schema-resolution path (where the
        # schema does not yet exist in the dbt graph state).
        results = run_dbt(["run"])
        assert len(results) >= 1

        # Run a second time: exercises the existing-schema path to confirm
        # duckdb__create_schema is idempotent and does not raise on a
        # second pass when the attached MySQL schema is already known.
        results = run_dbt(["run"])
        assert len(results) >= 1
