import pytest
import sqlite3
from pathlib import Path
from dbt.tests.util import (
    run_dbt,
)

model_sql = """
    {{ config(materialized='incremental', database='satest') }}
    select * from satest.tt1
"""

# Used by TestSQLitePluginNonMainSchemaRaises below. The 'schema' config
# forces a non-'main' target schema for an attached sqlite database, which
# must continue to raise a compiler error after #696's generalization of the
# duckdb__create_schema type check.
model_non_main_schema_sql = """
    {{ config(materialized='table', database='satest_nm', schema='not_main') }}
    select 1 as id
"""


class TestSQLitePlugin:

    @pytest.fixture(scope="class")
    def sqlite_test_db(self):
        path = '/tmp/satest.db'
        Path(path).unlink(missing_ok=True)
        db = sqlite3.connect(path)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE tt1 (id int, name text)")
        cursor.execute("INSERT INTO tt1 VALUES (1, 'John Doe')")
        cursor.execute("INSERT INTO tt1 VALUES (2, 'Jane Smith')")
        cursor.execute("CREATE TABLE test_table2 (a int, b int, c int)")
        cursor.execute("INSERT INTO test_table2 VALUES (1, 2, 3), (4, 5, 6)")
        cursor.close()
        db.commit()
        db.close()

        yield path

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, sqlite_test_db):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "attach": [
                           {'path': sqlite_test_db}
                        ]
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, test_data_path):
        return {
            "read_write.sql": model_sql,

        }

    def test_sqlite_plugin(self, project):
        results = run_dbt()
        assert len(results) == 1

        res = project.run_sql("SELECT COUNT(1) FROM satest.read_write", fetch="one")
        assert res[0] == 2


class TestSQLitePluginNonMainSchemaRaises:
    """
    Regression test for #696: when generalizing duckdb__create_schema to skip
    CREATE SCHEMA for all non-duckdb attached engines, the sqlite-specific
    'schema must be main' guard must still raise. The new macro recovers the
    attached type from duckdb_databases() and only raises when it is sqlite.
    """

    @pytest.fixture(scope="class")
    def sqlite_test_db(self):
        # filename determines the attached database name in duckdb, so we
        # use 'satest_nm.db' to land at catalog name 'satest_nm' and stay
        # disjoint from the sibling test class's 'satest' database.
        path = '/tmp/satest_nm.db'
        Path(path).unlink(missing_ok=True)
        db = sqlite3.connect(path)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE tt1 (id int)")
        cursor.close()
        db.commit()
        db.close()
        yield path

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, sqlite_test_db):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "attach": [
                            {'path': sqlite_test_db}
                        ]
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, test_data_path):
        return {
            "non_main.sql": model_non_main_schema_sql,
        }

    def test_sqlite_non_main_schema_raises(self, project):
        # The strict 'main' guard must still fire for sqlite attachments
        # after #696. The error is raised by the create_schema macro before
        # any model runs, so run_dbt propagates a CompilationError. Catch
        # broadly to stay portable across dbt-core versions.
        with pytest.raises(Exception) as exc_info:
            run_dbt(["run"])
        assert "Schema must be 'main' when writing to sqlite" in str(exc_info.value)


