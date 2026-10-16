import pytest
from dbt.tests.util import run_dbt


models__partitioned_and_sorted = """
{{ config(materialized='table', partitioned_by='ds', sorted_by='region') }}

select 1 as id, '2025-01-01' as ds, 'us' as region
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region
"""


@pytest.mark.skip_database_type(
    "duckdb", reason="This test validates DuckLake-specific table configuration"
)
class TestDuckLakeFeatures:
    @pytest.fixture(scope="class")
    def models(self):
        return {"partitioned_and_sorted.sql": models__partitioned_and_sorted}

    def test_partition_and_sort_order(self, project, profile_type):
        run_dbt(["run", "--select", "partitioned_and_sorted"])
        result = run_dbt(["run", "--select", "partitioned_and_sorted"])
        node = result.results[0].node

        relation = project.adapter.Relation.create(
            database=node.database,
            schema=node.schema,
            identifier=node.alias,
        )
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 2

        # Managed DuckLake does not expose its internal metadata schema.
        if profile_type == "md":
            return

        metadata_schema = f"__ducklake_metadata_{node.database}"

        partition_columns = project.run_sql(
            f"""
            select c.column_name
            from {metadata_schema}.ducklake_partition_column pc
            join {metadata_schema}.ducklake_column c
              on c.table_id = pc.table_id
             and c.column_id = pc.column_id
            join {metadata_schema}.ducklake_table t on t.table_id = c.table_id
            join {metadata_schema}.ducklake_schema s on s.schema_id = t.schema_id
            where lower(t.table_name) = lower('{node.alias}')
              and lower(s.schema_name) = lower('{node.schema}')
              and t.end_snapshot is null
              and c.end_snapshot is null
              and s.end_snapshot is null
            order by c.column_id
            """,
            fetch="all",
        )
        sort_expressions = project.run_sql(
            f"""
            select se.expression
            from {metadata_schema}.ducklake_sort_info si
            join {metadata_schema}.ducklake_sort_expression se
              on se.sort_id = si.sort_id
             and se.table_id = si.table_id
            join {metadata_schema}.ducklake_table t on t.table_id = si.table_id
            join {metadata_schema}.ducklake_schema s on s.schema_id = t.schema_id
            where lower(t.table_name) = lower('{node.alias}')
              and lower(s.schema_name) = lower('{node.schema}')
              and t.end_snapshot is null
              and s.end_snapshot is null
              and si.end_snapshot is null
            order by se.sort_key_index
            """,
            fetch="all",
        )

        assert [row[0].lower() for row in partition_columns] == ["ds"]
        assert [row[0].lower() for row in sort_expressions] == ["region"]
