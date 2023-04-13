import os
import pytest
import sqlite3

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

sources_schema_yml = """
version: 2
sources:
  - name: sql_source
    schema: main
    meta:
      plugin: sql
    tables:
      - name: tt1
        description: "My first SQLAlchemy table"
        meta:
          query: "SELECT * FROM {identifier} WHERE id=:id"
          params:
            id: 1
      - name: tt2
        meta:
          table: "test_table2"
"""

models_source_model1_sql = """
    select * from {{ source('sql_source', 'tt1') }}
"""
models_source_model2_sql = """
    select * from {{ source('sql_source', 'tt2') }}
"""


@pytest.mark.skip_profile("buenavista")
class TestSQLAlchemyPlugin:
    @pytest.fixture(scope="class")
    def sqlite_test_db(self):
        path = "/tmp/satest.db"
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
        os.unlink(path)

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, sqlite_test_db):
        config = {"connection_url": f"sqlite:///{sqlite_test_db}"}
        if "path" not in dbt_profile_target:
            return {}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target["path"],
                        "plugins": [
                            {"name": "sql", "impl": "sqlalchemy", "config": config}
                        ],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources_schema_yml,
            "source_model1.sql": models_source_model1_sql,
            "source_model2.sql": models_source_model2_sql,
        }

    def test_sqlalchemy_plugin(self, project):
        results = run_dbt()
        assert len(results) == 2

        check_relations_equal(
            project.adapter,
            [
                "tt1",
                "source_model1",
            ],
        )

        check_relations_equal(
            project.adapter,
            [
                "tt2",
                "source_model2",
            ],
        )
