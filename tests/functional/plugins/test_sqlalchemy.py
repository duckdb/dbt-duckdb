import os
import pytest
import sqlite3

from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
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
      query: "SELECT * FROM {identifier} WHERE id=:id"
    tables:
      - name: test_table
        description: "My first SQLAlchemy table"
        meta:
          params:
            id: 1
"""

models_source_model1_sql = """
    select * from {{ source('sql_source', 'test_table') }}
"""


class TestSQLAlchemyPlugin:
    @pytest.fixture(scope="class")
    def sqlite_test_db(self):
        path = "/tmp/satest.db"
        db = sqlite3.connect(path)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE test_table (id int, name text)")
        cursor.execute("INSERT INTO test_table VALUES (1, 'John Doe')")
        cursor.execute("INSERT INTO test_table VALUES (2, 'Jane Smith')")
        cursor.close()
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
        }

    def test_sqlalchemy_plugin(self, project):
        results = run_dbt()
        assert len(results) == 1

        check_relations_equal(
            project.adapter,
            [
                "test_table",
                "source_model1",
            ],
        )
