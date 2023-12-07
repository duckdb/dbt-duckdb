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


