import pytest
from dbt.tests.util import run_dbt
from dbt.adapters.duckdb import DuckDBConnectionManager

upstream_model_sql = """
select range from range(3)
"""


downstream_model_sql = """
select range * 2 from {{ ref('upstream_model') }}
"""

# class must begin with 'Test'
class TestRematerializeDownstreamExternalModel:
    """
    External models should load in dependencies when they exist.

    We test that after materializing upstream and downstream models, we can
    materialize the downstream model by itself, even if we are using an
    in-memory database.
    """

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "duckdb",
            "path": ":memory:",
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "base", "models": {"+materialized": "external"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.sql": upstream_model_sql,
            "downstream_model.sql": downstream_model_sql,
        }

    def test_run(self, project):
        run_dbt(["run"])

        # Force close the :memory: connection
        DuckDBConnectionManager.close_all_connections()
        run_dbt(["run", "--select", "downstream_model"])
