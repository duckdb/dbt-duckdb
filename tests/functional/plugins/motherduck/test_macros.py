"""
Test that the generate database name macro is case insensitive

See DuckDB docs: https://duckdb.org/docs/sql/dialect/keywords_and_identifiers.html

"Identifiers in DuckDB are always case-insensitive, similarly to PostgreSQL.
However, unlike PostgreSQL (and some other major SQL implementations), DuckDB also
treats quoted identifiers as case-insensitive."
"""
from urllib.parse import urlparse
import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name
)
from tests.functional.plugins.motherduck.fixtures import (
    models__gen_data_macro,
    macros__generate_database_name,
    macros__generate_schema_name,
    seeds__example_seed_csv,
)


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMacrosGenerateDatabaseName:
    @pytest.fixture(scope="class")
    def database_name(self, dbt_profile_target, request):
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
        return {
            "db_name.sql": macros__generate_database_name,
            "schema_name.sql": macros__generate_schema_name
        }
    
    @staticmethod
    def gen_project_config_update(build_env, org_prefix):
        return {
            "config-version": 2,
            "vars": {
                "test": {
                    "build_env": build_env,
                    "org_prefix": org_prefix
                },
            },
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return self.gen_project_config_update("ducky", "ducky")

    def test_dbname_macro(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["seed"])

        for _ in range(3):
            results = run_dbt(["run"])
            assert len(results) == 1
            check_result_nodes_by_name(results, ["model"])


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMacrosGenerateDatabaseNameUpperCase(TestMacrosGenerateDatabaseName):
    @pytest.fixture(scope="class")
    def database_name(self, dbt_profile_target, request):
        return urlparse(dbt_profile_target["path"]).path + "_ducky_ducky"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return self.gen_project_config_update("DUCKY", "DUCKY")


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMacrosGenerateDatabaseNameLowerCase(TestMacrosGenerateDatabaseName):
    @pytest.fixture(scope="class")
    def database_name(self, dbt_profile_target, request):
        return urlparse(dbt_profile_target["path"]).path + "_DUCKY_DUCKY"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return self.gen_project_config_update("ducky", "ducky")


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMacrosGenerateDatabaseNameAllMixedCase(TestMacrosGenerateDatabaseName):
    @pytest.fixture(scope="class")
    def database_name(self, dbt_profile_target, request):
        return urlparse(dbt_profile_target["path"]).path + "_dUcKy_DUckY"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return self.gen_project_config_update("DuCkY", "dUcKy")
