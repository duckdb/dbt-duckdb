import pytest

from dbt.tests.fixtures.project import TestProjInfo


def _create_config(dbt_profile_target, sql_from: str) -> dict:
    return {
        "test": {
            "outputs": {
                "dev": {
                    "type": "duckdb",
                    "path": dbt_profile_target.get("path", ":memory:"),
                    "plugins": [
                        {"module": "sqlglot", "config": {"sql_from": sql_from}}
                    ],
                }
            },
            "target": "dev",
        }
    }


class TestSqlglotPluginPostgres:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return _create_config(dbt_profile_target, "postgres")

    def test_sqlglot_plugin(self, project: TestProjInfo):
        res = project.run_sql("select to_date('2024-12-31', 'YYYY-MM-DD')", fetch="one")


class TestSqlglotPluginMySql:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return _create_config(dbt_profile_target, "mysql")

    def test_sqlglot_plugin(self, project: TestProjInfo):
        project.test_config
        res = project.run_sql(
            "select str_to_date('2024-12-31', '%Y-%m-%d')", fetch="one"
        )
