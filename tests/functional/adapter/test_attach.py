import os

import duckdb
import pytest
import yaml

from dbt.tests.util import run_dbt

sources_schema_yml = """version: 2
sources:
  - name: attached_source
    database: attach_test
    schema: analytics
    tables:
      - name: attached_table
        description: "An attached table"
        columns:
          - name: id
            description: "An id"
            tests:
              - unique
              - not_null
"""

models_source_model_sql = """select * from {{ source('attached_source', 'attached_table') }}
"""

models_target_model_sql = """
    {{ config(materialized='table', database='attach_test') }}
    SELECT * FROM {{ ref('source_model') }}
"""


class TestAttachedDatabase:
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        db = duckdb.connect("/tmp/attach_test.duckdb")
        db.execute("CREATE SCHEMA analytics")
        db.execute("CREATE TABLE analytics.attached_table AS SELECT 1 as id")
        db.commit()
        db.close()

        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": ":memory:",
                        "attach": [{"path": "/tmp/attach_test.duckdb"}],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources_schema_yml,
            "source_model.sql": models_source_model_sql,
            "target_model.sql": models_target_model_sql,
        }

    def test_attached_databases(self, project):
        try:
            results = run_dbt()
            assert len(results) == 2

            test_results = run_dbt(["test"])
            assert len(test_results) == 2

            db = duckdb.connect("/tmp/attach_test.duckdb")
            ret = db.execute(f"SELECT * FROM target_model").fetchall()
            assert ret[0][0] == 1
        finally:
            os.unlink("/tmp/attach_test.duckdb")
