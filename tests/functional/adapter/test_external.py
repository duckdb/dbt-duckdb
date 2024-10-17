import os
import pytest
from dbt.tests.adapter.basic.files import (
    base_table_sql,
    model_base,
    schema_base_yml,
    seeds_base_csv,
)
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)

config_materialized_default = """
  {{ config(materialized="external") }}
"""

config_materialized_csv = """
  {{ config(materialized="external", format="csv") }}
"""

config_materialized_parquet_location = """
  {{ config(materialized="external", location="{{ adapter.external_root() }}/test.parquet") }}
"""

config_materialized_csv_location_delim = """
  {{ config(materialized="external", location="{{ adapter.external_root() }}/test_delim.csv", delimiter="|") }}
"""

config_json = """
  {{ config(materialized="external", format="json") }}
"""

default_external_sql = config_materialized_default + model_base
csv_external_sql = config_materialized_csv + model_base
parquet_table_location_sql = config_materialized_parquet_location + model_base
csv_location_delim_sql = config_materialized_csv_location_delim + model_base
json_sql = config_json + model_base


class BaseExternalMaterializations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": base_table_sql,
            "table_default.sql": default_external_sql,
            "table_csv.sql": csv_external_sql,
            "table_parquet_location.sql": parquet_table_location_sql,
            "table_csv_location_delim.sql": csv_location_delim_sql,
            "table_json.sql": json_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "external")
        os.mkdir(extroot)
        dbt_profile_target["external_root"] = extroot
        return dbt_profile_target
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_base(self, project):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 6

        # names exist in result nodes
        check_result_nodes_by_name(
            results,
            [
                "table_model",
                "table_default",
                "table_csv",
                "table_parquet_location",
                "table_csv_location_delim",
                "table_json",
            ],
        )

        # check relation types
        expected = {
            "base": "table",
            "table_model": "table",
            "table_default": "view",
            "table_parquet_location": "view",
            "table_csv": "view",
            "table_csv_location_delim": "view",
            "table_json": "view",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(
            project.adapter,
            [
                "base",
                "table_default",
                "table_parquet_location",
                "table_model",
                "table_csv",
                "table_csv_location_delim",
                "table_json",
            ],
        )

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 7
        assert len(catalog.sources) == 1


class TestExternalMaterializations(BaseExternalMaterializations):
    pass
