import os
import tempfile

import duckdb
import pytest

from dbt.tests.util import run_dbt

sources_schema_yml = """
version: 2
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

models_source_model_sql = """
    select * from {{ source('attached_source', 'attached_table') }}
"""

models_target_model_sql = """
    {{ config(materialized='table', database='attach_test') }}
    SELECT * FROM {{ ref('source_model') }}
"""


@pytest.mark.skip_profile("memory", "buenavista", "md")
class TestAttachedDatabase:
    @pytest.fixture(scope="class")
    def attach_test_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "attach_test.duckdb")
            db = duckdb.connect(path)
            db.execute("CREATE SCHEMA analytics")
            db.execute("CREATE TABLE analytics.attached_table AS SELECT 1 as id")
            db.close()
            yield path

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, attach_test_db):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "attach": [{"path": attach_test_db}],
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

    def test_attached_databases(self, project, attach_test_db):
        results = run_dbt()
        assert len(results) == 2

        test_results = run_dbt(["test"])
        assert len(test_results) == 2

        # check that the model is created in the attached db
        db = duckdb.connect(attach_test_db)
        ret = db.execute("SELECT * FROM target_model").fetchall()
        assert ret[0][0] == 1
        db.close()

        # check that everything works on a re-run of dbt
        rerun_results = run_dbt()
        assert len(rerun_results) == 2
