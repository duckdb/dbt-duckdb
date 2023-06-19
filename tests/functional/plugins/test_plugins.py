import os
import pytest
import sqlite3

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

excel_schema_yml = """
version: 2
sources:
  - name: excel_source
    schema: main
    meta:
      plugin: excel
    tables:
      - name: excel_file
        description: "An excel file"
        meta:
          external_location: "{test_data_path}/excel_file.xlsx"
"""

sqlalchemy_schema_yml = """
version: 2
sources:
  - name: sql_source
    schema: main
    config:
      plugin: sql
      save_mode: ignore
    tables:
      - name: tt1
        description: "My first SQLAlchemy table"
        config:
          query: "SELECT * FROM {identifier} WHERE id=:id"
          params:
            id: 1
      - name: tt2
        config:
          table: "test_table2"
"""


excel1_sql = """
    select * from {{ source('excel_source', 'excel_file') }}
"""
sqlalchemy1_sql = """
    select * from {{ source('sql_source', 'tt1') }}
"""
sqlalchemy2_sql = """
   {{ config(materialized='external', plugin='sql') }}
    select * from {{ source('sql_source', 'tt2') }}
"""
foo_sql = """
    select foo() as foo
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
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

        # verify that the external plugin operation works to write to the db
        db = sqlite3.connect(path)
        cursor = db.cursor()
        res = cursor.execute("SELECT * FROM sqlalchemy2").fetchall()
        assert len(res) == 2
        assert res[0] == (1, 2, 3)
        assert res[1] == (4, 5, 6)
        cursor.close()
        db.close()
        os.unlink(path)

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, sqlite_test_db):
        config = {"connection_url": f"sqlite:///{sqlite_test_db}"}
        plugins = [
            {"module": "excel"},
            {"module": "sqlalchemy", "alias": "sql", "config": config},
            {"module": "tests.create_function_plugin"},
        ]

        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": plugins,
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, test_data_path):
        return {
            "schema_excel.yml": excel_schema_yml.format(test_data_path=test_data_path),
            "schema_sqlalchemy.yml": sqlalchemy_schema_yml,
            "excel.sql": excel1_sql,
            "sqlalchemy1.sql": sqlalchemy1_sql,
            "sqlalchemy2.sql": sqlalchemy2_sql,
            "foo.sql": foo_sql,
        }

    def test_plugins(self, project, test_data_path):
        results = run_dbt()
        assert len(results) == 4

        res = project.run_sql("SELECT COUNT(1) FROM excel_file", fetch="one")
        assert res[0] == 9

        check_relations_equal(
            project.adapter,
            [
                "excel_file",
                "excel",
            ],
        )

        res = project.run_sql("SELECT COUNT(1) FROM tt1", fetch="one")
        assert res[0] == 1
        check_relations_equal(
            project.adapter,
            [
                "tt1",
                "sqlalchemy1",
            ],
        )

        res = project.run_sql("SELECT COUNT(1) FROM tt2", fetch="one")
        assert res[0] == 2
        check_relations_equal(
            project.adapter,
            [
                "tt2",
                "sqlalchemy2",
            ],
        )

        res = project.run_sql("SELECT foo FROM foo", fetch="one")
        assert res[0] == 1729
