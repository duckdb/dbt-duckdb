import copy
import os

import pytest

from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.tests.util import run_dbt

seeds__people_csv = """id,name
1,alice
2,bob
3,carol
""".lstrip()

models__parquet_sql = """
{{ config(materialized="external") }}
select * from {{ ref('people') }}
"""

models__partitioned_csv_sql = """
{{ config(materialized="external", format="csv", options={"partition_by": "id"}) }}
select * from {{ ref('people') }}
"""

models__from_source_sql = """
{{ config(materialized="table") }}
select * from {{ source('files', 'people_parquet') }}
"""

models__downstream_sql = """
{{ config(materialized="table") }}
select * from {{ ref('people_partitioned') }}
"""

sources__yml = """
sources:
  - name: files
    meta:
      external_location: "{{ env_var('DBT_DUCKDB_TEST_EXTERNAL_ROOT') }}/{name}.parquet"
    tables:
      - name: people_parquet
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestPathsWithSingleQuotes:
    """Seed files and external locations under a directory whose name has a single quote."""

    @pytest.fixture(scope="class")
    def project_root(self, tmpdir_factory):
        return tmpdir_factory.mktemp("o'brien's project")

    @pytest.fixture(scope="class")
    def external_root(self, project_root):
        extroot = os.path.join(str(project_root), "external")
        os.makedirs(extroot, exist_ok=True)
        os.environ["DBT_DUCKDB_TEST_EXTERNAL_ROOT"] = extroot
        yield extroot
        del os.environ["DBT_DUCKDB_TEST_EXTERNAL_ROOT"]

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, external_root):
        target = copy.deepcopy(dbt_profile_target)
        target["external_root"] = external_root
        return target

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"on-run-start": ["{{ register_upstream_external_models() }}"]}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"people.csv": seeds__people_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_parquet.sql": models__parquet_sql,
            "people_partitioned.sql": models__partitioned_csv_sql,
            "people_from_source.sql": models__from_source_sql,
            "people_downstream.sql": models__downstream_sql,
            "sources.yml": sources__yml,
        }

    def test_paths_with_single_quotes(self, project, external_root):
        assert "'" in external_root

        run_dbt(["seed"])

        run_dbt(["run", "--exclude", "people_from_source"])
        assert os.path.isfile(os.path.join(external_root, "people_parquet.parquet"))
        assert os.path.isdir(os.path.join(external_root, "people_partitioned"))

        # reads the file people_parquet wrote through the source's external_location
        run_dbt(["run", "--select", "people_from_source"])

        def assert_people(relation):
            rows = project.run_sql(
                f"select id, name from {project.test_schema}.{relation} order by id",
                fetch="all",
            )
            assert [tuple(row) for row in rows] == [
                (1, "alice"),
                (2, "bob"),
                (3, "carol"),
            ], relation

        for relation in (
            "people",
            "people_parquet",
            "people_partitioned",
            "people_from_source",
            "people_downstream",
        ):
            assert_people(relation)

        with project.adapter.connection_named("_test"):
            assert project.adapter.location_exists(
                os.path.join(external_root, "people_parquet.parquet")
            )
            assert not project.adapter.location_exists(
                os.path.join(external_root, "missing.parquet")
            )

        # with the :memory: views gone, the downstream model reads the upstream
        # external model through the view register_upstream_external_models creates
        DuckDBConnectionManager.close_all_connections()
        run_dbt(["run", "--select", "people_downstream"])
        assert_people("people_downstream")
