import os
import pytest
from dbt.tests.util import run_dbt, relation_from_name
from dbt.adapters.duckdb import DuckDBConnectionManager

upstream_model_sql = """
select range from range(3)
"""

upstream_partition_by_model = """
{{ config(materialized='external', options={"partition_by": "a"}) }}
select range as a, 'foo' as b from range(5)
"""

downstream_model_sql = """
select range * 2 from {{ ref('upstream_model') }}
"""

other_downstream_model_sql = """
select range * 5 from {{ ref('upstream_model') }}
"""

downstream_of_partition_model = """
select a from {{ ref('upstream_partition_by_model') }}
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
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "rematerialize")
        os.mkdir(extroot)
        dbt_profile_target["external_root"] = extroot
        return dbt_profile_target
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"+materialized": "external"},
            "on-run-start": ["{{ register_upstream_external_models() }}"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.sql": upstream_model_sql,
            "upstream_partition_by_model.sql": upstream_partition_by_model,
            "downstream_model.sql": downstream_model_sql,
            "other_downstream_model.sql": other_downstream_model_sql,
            "downstream_of_partition_model.sql": downstream_of_partition_model,
        }

    def test_run(self, project):
        run_dbt(["run"])

        # Force close the :memory: connection
        DuckDBConnectionManager.close_all_connections()
        run_dbt(
            [
                "run",
                "--select",
                "downstream_model other_downstream_model downstream_of_partition_model",
            ]
        )

        # really makes sure we have created the downstream model
        relation = relation_from_name(project.adapter, "downstream_of_partition_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 5
