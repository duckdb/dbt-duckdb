import pytest
from dbt.tests.util import run_dbt

from tests.ducklake import BaseDucklakeIntegration
from tests.ducklake import ducklake_metadata_schema


@pytest.fixture(scope="class")
def models__table_model(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}') }}}}
select
    {{% if var('fail', false) %}}
        cast(error('boom') as integer)
    {{% else %}}
        {{{{ var('marker', 10) }}}}
    {{% endif %}} as id
"""


def active_table_path(project, node):
    metadata_schema = ducklake_metadata_schema(node.database)
    row = project.run_sql(
        f"""
        select t.path
        from {metadata_schema}.ducklake_table t
        join {metadata_schema}.ducklake_schema s
          on s.schema_id = t.schema_id
        where lower(t.table_name) = lower('{node.alias}')
          and lower(s.schema_name) = lower('{node.schema}')
          and t.end_snapshot is null
          and s.end_snapshot is null
        """,
        fetch="one",
    )
    return row[0]


def select_ids(project, node):
    relation = project.adapter.Relation.create(
        database=node.database,
        schema=node.schema,
        identifier=node.alias,
    )
    rows = project.run_sql(f"select id from {relation}", fetch="all")
    return [tuple(row) for row in rows]


class TestDucklakeTablePaths(BaseDucklakeIntegration):
    @pytest.fixture(scope="class")
    def models(self, models__table_model):
        return {"table_model.sql": models__table_model}

    def test_table_is_stored_under_its_own_name(self, project):
        result = run_dbt(["run", "--select", "table_model", "--vars", "{marker: 7}"])
        node = result.results[0].node
        expected_path = f"{node.alias}/"
        assert active_table_path(project, node) == expected_path
        run_dbt(["run", "--select", "table_model"])
        assert active_table_path(project, node) == expected_path
        assert select_ids(project, node) == [(10,)]

    def test_failed_rebuild_keeps_old_rows(self, project):
        result = run_dbt(["run", "--select", "table_model", "--vars", "{marker: 7}"])
        node = result.results[0].node
        expected_path = f"{node.alias}/"

        run_dbt(
            ["run", "--select", "table_model", "--vars", "{fail: true}"],
            expect_pass=False,
        )
        assert select_ids(project, node) == [(7,)]
        assert active_table_path(project, node) == expected_path
        run_dbt(["run", "--select", "table_model"])
        assert select_ids(project, node) == [(10,)]
        assert active_table_path(project, node) == expected_path
