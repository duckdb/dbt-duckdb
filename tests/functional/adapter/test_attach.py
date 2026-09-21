import os
import tempfile

import duckdb
import pytest

from dbt.adapters.duckdb import DuckDBConnectionManager
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

        DuckDBConnectionManager.close_all_connections()

        # check that the model is created in the attached db
        db = duckdb.connect(attach_test_db)
        ret = db.execute("SELECT * FROM target_model").fetchall()
        assert ret[0][0] == 1
        db.close()

        # check that everything works on a re-run of dbt
        rerun_results = run_dbt()
        assert len(rerun_results) == 2


indexed_model_sql = """
    {{ config(
        materialized='table',
        database='attach_test',
        indexes=[{'columns': ['id']}],
    ) }}
    SELECT 1 as id
"""


@pytest.mark.skip_profile("memory", "buenavista", "md")
class TestIndexOnAttachedDatabase:
    """Regression test for #771: dropping indexes on a table in an attached
    (non-default) catalog requires the full three-part database.schema.index
    path. The second dbt run exercises the DROP INDEX path in
    drop_indexes_on_relation."""

    @pytest.fixture(scope="class")
    def attach_test_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "attach_test.duckdb")
            db = duckdb.connect(path)
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
        return {"indexed_model.sql": indexed_model_sql}

    def test_index_on_attached_database(self, project):
        # First run creates the table + index (no DROP INDEX path).
        results = run_dbt()
        assert len(results) == 1

        # Second run re-materializes the table, which drops the existing
        # index first. Without the database_name prefix this fails with
        # "Catalog Error: Index ... does not exist!".
        rerun_results = run_dbt()
        assert len(rerun_results) == 1


fk_schema_yml = """
version: 2

models:
  - name: fk_target
    config:
      materialized: table
      contract:
        enforced: true
    columns:
      - name: col
        data_type: int
        constraints:
          - type: primary_key
      - name: extra
        data_type: varchar

  - name: fk_source
    config:
      materialized: table
      contract:
        enforced: true
    columns:
      - name: col
        data_type: int
        constraints:
          - type: foreign_key
            expression: "main.fk_target (col)"
      - name: extra
        data_type: varchar
"""

fk_source_model_sql = """
    -- depends_on: {{ ref("fk_target") }}
    select unnest([1, 1]) as col, unnest(['a', 'b']) as extra
"""

fk_target_model_sql = """
    select unnest([1, 2]) as col, unnest(['blah', 'blah']) as extra
"""


@pytest.mark.skip_profile("memory", "buenavista", "md")
class TestForeignKeyOnAttachedDatabaseAlias:
    """Regression test for #623: when `database` in the profile is set to
    match one of the `attach` aliases (routing all model materialization
    into that attached database instead of the primary one), a model with
    a FOREIGN KEY constraint failed with "Catalog Error: Table ... does not
    exist" because the target of the FK -- necessarily unqualified, since
    DuckDB rejects a catalog-qualified FOREIGN KEY target even when it
    matches the current catalog -- was resolved against the primary
    (path-derived) catalog rather than the attached one. A DuckDB cursor
    does not inherit a `USE` issued on its parent connection, so every
    per-model cursor defaulted back to the primary catalog unless `USE`
    was reissued on the cursor itself."""

    @pytest.fixture(scope="class")
    def attach_test_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "attach_test.duckdb")
            db = duckdb.connect(path)
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
                        "database": "attach_test",
                        "attach": [{"path": attach_test_db, "alias": "attach_test"}],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fk_schema_yml,
            "fk_source.sql": fk_source_model_sql,
            "fk_target.sql": fk_target_model_sql,
        }

    def test_foreign_key_on_attached_database_alias(self, project):
        results = run_dbt()
        assert len(results) == 2
