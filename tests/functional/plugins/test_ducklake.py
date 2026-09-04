import pytest

from dbt.tests.util import run_dbt

events_sql = """
{{ config(materialized='external', plugin='ducklake') }}
select 1 as id, 'a' as val union all select 2 as id, 'b' as val
"""

overrides_sql = """
{{ config(materialized='external', plugin='ducklake', ducklake_schema='raw', ducklake_table='renamed') }}
select 3 as id
"""


@pytest.mark.skip_profile("buenavista", "md", "nightly")
class TestDuckLakePlugin:
    @pytest.fixture(scope="class")
    def lake_dirs(self, tmp_path_factory):
        base = tmp_path_factory.mktemp("ducklake_plugin")
        (base / "external").mkdir()
        return str(base / "lake.ducklake"), str(base / "external")

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, lake_dirs):
        catalog, external_root = lake_dirs
        dbt_profile_target["extensions"] = ["ducklake"]
        dbt_profile_target["external_root"] = external_root
        dbt_profile_target["attach"] = [{"path": f"ducklake:{catalog}", "alias": "lake"}]
        dbt_profile_target["plugins"] = [{"module": "ducklake", "config": {"database": "lake"}}]
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def models(self):
        return {"events.sql": events_sql, "overrides.sql": overrides_sql}

    def test_register_parquet_in_ducklake(self, project):
        project.run_sql("create schema lake.raw")
        results = run_dbt()
        assert len(results) == 2

        rows = project.run_sql("select count(*), min(id), max(id) from lake.events", fetch="one")
        assert rows == (2, 1, 2)
        # the parquet file is registered, not copied
        files = project.run_sql(
            "select count(*) from ducklake_list_files('lake', 'events')", fetch="one"
        )
        assert files[0] == 1

        rows = project.run_sql("select id from lake.raw.renamed", fetch="one")
        assert rows == (3,)
