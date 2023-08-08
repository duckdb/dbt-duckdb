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

config_write_csv_delim_options = """
  {{ config(materialized="external", format="csv", options={"delimiter": "|"}) }}
"""

config_write_codec_options = """
  {{ config(materialized="external", options={"codec": "zstd"}) }}
"""

config_write_partition_by_id = """
    {{ config(materialized="external", options={"partition_by": "id", "codec": "zstd"}) }}
"""

config_write_partition_by_id_name = """
    {{ config(materialized="external", options={"partition_by": "id, name"}) }}
"""

csv_delim_options_sql = config_write_csv_delim_options + model_base
write_codec_options = config_write_codec_options + model_base
config_write_partition_by_id_sql = config_write_partition_by_id + model_base
config_write_partition_by_id_name_sql = config_write_partition_by_id_name + model_base


class BaseExternalMaterializations:

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "write_options")
        os.mkdir(extroot)
        dbt_profile_target["external_root"] = extroot
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": base_table_sql,
            "csv_delim_options.sql": csv_delim_options_sql,
            "write_codec_options.sql": write_codec_options,
            "config_write_partition_by_id.sql": config_write_partition_by_id_sql,
            "config_write_partition_by_id_name.sql": config_write_partition_by_id_name_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

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
        assert len(results) == 5

        # names exist in result nodes
        check_result_nodes_by_name(
            results,
            [
                "table_model",
                "csv_delim_options",
                "write_codec_options",
                "config_write_partition_by_id",
                "config_write_partition_by_id_name",
            ],
        )

        # check relation types
        expected = {
            "base": "table",
            "table_model": "table",
            "csv_delim_options": "view",
            "write_codec_options": "view",
            "config_write_partition_by_id": "view",
            "config_write_partition_by_id_name": "view",
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
                "csv_delim_options",
                "write_codec_options",
                "config_write_partition_by_id",
                "config_write_partition_by_id_name",
            ],
        )

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 6
        assert len(catalog.sources) == 1


class TestExternalMaterializations(BaseExternalMaterializations):
    pass
