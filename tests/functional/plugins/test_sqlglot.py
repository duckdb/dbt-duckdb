import pytest

from dbt.adapters.duckdb.plugins.postgres import Plugin
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import check_relations_equal, run_dbt


class TestSqlglotPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": [
                            {"module": "sqlglot", "config": {"trans_from": "postgres"}}
                        ],
                    }
                },
                "target": "dev",
            }
        }
    

    
    def test_sqlglot_plugin(self, project: TestProjInfo):
        res = project.run_sql("select to_date('2024-12-31', 'YYYY-MM-DD')", fetch="one")



class TestSqlglotPluginMySql:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": [
                            {"module": "sqlglot", "config": {"trans_from": "mysql"}}
                        ],
                    }
                },
                "target": "dev",
            }
        }
    

    
    def test_sqlglot_plugin(self, project: TestProjInfo):
        res = project.run_sql("select str_to_date('2024-12-31', '%Y-%m-%d')", fetch="one")