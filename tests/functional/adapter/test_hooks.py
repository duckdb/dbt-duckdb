import os
import pytest
from dbt.tests.util import run_dbt, relation_from_name

basic_model_sql = """
select range from range(3)
"""

post_hook_sql = """
set TimeZone to 'America/Los_Angeles'
"""


class TestPostHook:
    """
    Post hook should run inside txn
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": post_hook_sql}]},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": basic_model_sql,
        }

    def test_run(self, project):
        default_timezone = project.run_sql(
            "select value from duckdb_settings() where name = 'TimeZone';", fetch="one"
        )[0]

        # set timezone to NY
        result = project.run_sql(
            "set TimeZone to 'America/New_York'; select value from duckdb_settings() where name = 'TimeZone';",
            fetch="one",
        )
        assert result[0] == "America/New_York"

        run_dbt(["run"])

        # check that the model was run
        relation = relation_from_name(project.adapter, "basic_model")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 3

        # check that the post hook was run
        result = project.run_sql(
            "select value from duckdb_settings() where name = 'TimeZone'", fetch="one"
        )
        assert result[0] == "America/Los_Angeles"

        # reset
        result = project.run_sql(f"set TimeZone to '{default_timezone}'")


class TestPostHookTransactionFalse(TestPostHook):
    """
    Post hook should run outside txn
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": post_hook_sql, "transaction": False}]},
        }
