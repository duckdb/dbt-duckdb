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


upstream_csv_gzip_model = """
{{ config(materialized='external', format='csv', options={'compression': 'gzip'}) }}
select range as id, 'name_' || range as name from range(3)
"""

upstream_csv_no_header_model = """
{{ config(
    materialized='external',
    format='csv',
    options={'header': 'false'},
    csv_read_options={'header': 'false', 'auto_detect': 'true'},
) }}
select 'a' || range as a, 'b' || range as b from range(3)
"""

upstream_txt_location_model = """
{{ config(
    materialized='external',
    format='csv',
    location="{{ adapter.external_root() }}/upstream_txt_location.txt",
) }}
select range as id from range(4)
"""

downstream_of_csv_gzip_model = """
select id, name from {{ ref('upstream_csv_gzip_model') }}
"""

downstream_of_csv_no_header_model = """
select * from {{ ref('upstream_csv_no_header_model') }}
"""

downstream_of_txt_location_model = """
select id from {{ ref('upstream_txt_location_model') }}
"""


class TestRematerializeDownstreamExternalReadOptions:
    """
    The upstream views that register_upstream_external_models creates must read
    the external files the same way the external materialization does: with the
    write compression, the configured read options and a rendered location.
    """

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "rematerialize_read_options")
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
            "upstream_csv_gzip_model.sql": upstream_csv_gzip_model,
            "upstream_csv_no_header_model.sql": upstream_csv_no_header_model,
            "upstream_txt_location_model.sql": upstream_txt_location_model,
            "downstream_of_csv_gzip_model.sql": downstream_of_csv_gzip_model,
            "downstream_of_csv_no_header_model.sql": downstream_of_csv_no_header_model,
            "downstream_of_txt_location_model.sql": downstream_of_txt_location_model,
        }

    def test_run(self, project):
        run_dbt(["run"])

        # Force close the :memory: connection so the upstream views are gone
        DuckDBConnectionManager.close_all_connections()
        run_dbt(
            [
                "run",
                "--select",
                "downstream_of_csv_gzip_model downstream_of_csv_no_header_model"
                " downstream_of_txt_location_model",
            ]
        )

        expected = {
            "downstream_of_csv_gzip_model": 3,
            "downstream_of_csv_no_header_model": 3,
            "downstream_of_txt_location_model": 4,
        }
        for name, num_rows in expected.items():
            relation = relation_from_name(project.adapter, name)
            result = project.run_sql(f"select count(*) from {relation}", fetch="one")
            assert result[0] == num_rows, name

        # the headerless file is read back without treating its first row as a header
        relation = relation_from_name(project.adapter, "downstream_of_csv_no_header_model")
        result = project.run_sql(f"select min(column0) from {relation}", fetch="one")
        assert result[0] == "a0"
