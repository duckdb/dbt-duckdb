from urllib.parse import urlparse
import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name
)
from tests.functional.plugins.motherduck.fixtures import (
    models__gen_data_macro,
    macros__generate_database_name,
    seeds__example_seed_csv,
)


class TestMacrosGenerateDatabaseName:
    @pytest.fixture(scope="class")
    def database_name(self, dbt_profile_target):
        return urlparse(dbt_profile_target["path"]).path + "_ducky_ducky"

    @pytest.fixture(autouse=True)
    def run_dbt_scope(self, project, database_name):
        project.run_sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        yield
        project.run_sql(f"DROP DATABASE {database_name}")

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seeds__example_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models__gen_data_macro
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"db_name.sql": macros__generate_database_name}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "test": {
                    "build_env": "DUCKY",
                    "org_prefix": "DUCKY"
                },
            },
            "macro-paths": ["macros"],
        }

    def test_dbname_macro(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["seed"])

        # run command
        results = run_dbt(["run"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["model"])

        # run second time
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert """Catalog Error: Could not rename "model__dbt_tmp" to "model": another entry with this name already exists!""" \
        in results[0].message
