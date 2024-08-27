import os
import pytest
from dbt.tests.adapter.basic.files import (
    base_table_sql,
    model_base,
    schema_base_yml,
    seeds_base_csv,
)
from dbt.tests.util import (
    run_dbt,
)

config_materialized_glue = """
  {{ config(materialized="external", glue_register=true, glue_database='db2') }}
"""
default_glue_sql = config_materialized_glue + model_base


@pytest.mark.skip
class TestGlueMaterializations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": base_table_sql,
            "table_default.sql": default_glue_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        dbt_profile_target["external_root"] = "s3://duckdbtest/glue_test"
        dbt_profile_target["extensions"] = [{"name": "httpfs"}]
        dbt_profile_target["settings"] = {
            "s3_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "s3_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "s3_region": "us-west-2",
        }
        return dbt_profile_target

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 2
