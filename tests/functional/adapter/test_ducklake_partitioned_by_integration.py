import pytest
from dbt.tests.util import run_dbt

from tests.ducklake import BaseDucklakeIntegration
from tests.ducklake import ducklake_metadata_schema


@pytest.fixture(scope="class")
def models__table_partitioned_model(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', partitioned_by='ds') }}}}

select 1 as id, '2025-01-01' as ds, 'us' as region, 10 as amount
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region, 20 as amount
"""


@pytest.fixture(scope="class")
def models__incremental_partitioned_model(test_database_name):
    return f"""
{{{{ config(
    materialized='incremental',
    database='{test_database_name}',
    unique_key='id',
    partition_by=['ds', 'region']
) }}}}

{{% if is_incremental() %}}
select 2 as id, '2025-01-02' as ds, 'eu' as region, 22 as amount
union all
select 3 as id, '2025-01-03' as ds, 'ca' as region, 30 as amount
{{% else %}}
select 1 as id, '2025-01-01' as ds, 'us' as region, 10 as amount
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region, 20 as amount
{{% endif %}}
"""


@pytest.fixture(scope="class")
def models__python_partitioned_model(test_database_name):
    return f"""
import pandas as pd


def model(dbt, _):
    dbt.config(materialized='table', database='{test_database_name}', partitioned_by='ds')
    return pd.DataFrame(
        {{
            "id": [1, 2],
            "ds": ["2025-01-01", "2025-01-02"],
            "region": ["us", "eu"],
            "amount": [10, 20],
        }}
    )
"""


@pytest.fixture(scope="class")
def models__non_ducklake_partitioned_table():
    return """
{{ config(materialized='table', partitioned_by='ds') }}

select 1 as ds, 'a' as value
union all
select 2 as ds, 'b' as value
"""


@pytest.fixture(scope="class")
def models__invalid_partitioned_by(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', partitioned_by=['ds', 1]) }}}}

select 1 as ds, 'a' as value
"""


@pytest.fixture(scope="class")
def models__empty_partitioned_by_list(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', partitioned_by=[]) }}}}

select 1 as ds, 'a' as value
"""


@pytest.fixture(scope="class")
def models__transform_partitioned_model(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', partitioned_by=['day(ts)', 'region']) }}}}

select TIMESTAMP '2025-01-01 00:00:00' as ts, 'us' as region, 1 as id
union all
select TIMESTAMP '2025-01-02 00:00:00' as ts, 'eu' as region, 2 as id
"""


def get_partition_columns_with_transforms(project, node):
    """Returns partition metadata as a tuple ([columns...], [transforms...])

    For example, (["c1", "c2"], ["day", "identity"]) means that column
    c1 uses the day transform and c2 the identity transform.
    """
    metadata_schema = ducklake_metadata_schema(node.database)
    query = f"""
        select c.column_name, pc.transform
        from {metadata_schema}.ducklake_partition_column pc
        join {metadata_schema}.ducklake_column c
          on c.table_id = pc.table_id
         and c.column_id = pc.column_id
        join {metadata_schema}.ducklake_table t
          on t.table_id = c.table_id
        join {metadata_schema}.ducklake_schema s
          on s.schema_id = t.schema_id
        where lower(t.table_name) = lower('{node.alias}')
          and lower(s.schema_name) = lower('{node.schema}')
          and t.end_snapshot is null
          and c.end_snapshot is null
          and s.end_snapshot is null
        order by c.column_id
    """
    rows = project.run_sql(query, fetch="all")
    return [row[0].lower() for row in rows], [str(row[1]).lower() for row in rows]


class TestDucklakePartitionedByIntegration(BaseDucklakeIntegration):
    @pytest.fixture(scope="class")
    def models(
        self,
        models__table_partitioned_model,
        models__incremental_partitioned_model,
        models__python_partitioned_model,
        models__transform_partitioned_model,
    ):
        return {
            "table_partitioned_model.sql": models__table_partitioned_model,
            "incremental_partitioned_model.sql": models__incremental_partitioned_model,
            "python_partitioned_model.py": models__python_partitioned_model,
            "transform_partitioned_model.sql": models__transform_partitioned_model,
        }

    def test_table_partitioned_by_sets_partition_columns(self, project):
        result = run_dbt(["run", "--select", "table_partitioned_model"], expect_pass=True)
        partition_columns, _ = get_partition_columns_with_transforms(
            project, result.results[0].node
        )
        assert partition_columns == ["ds"]

    def test_table_partitioned_by_is_idempotent(self, project):
        run_dbt(["run", "--select", "table_partitioned_model"], expect_pass=True)
        result = run_dbt(["run", "--select", "table_partitioned_model"], expect_pass=True)
        partition_columns, _ = get_partition_columns_with_transforms(
            project, result.results[0].node
        )
        assert partition_columns == ["ds"]

    def test_incremental_partition_by_sets_partition_columns(self, project):
        run_dbt(["run", "--select", "incremental_partitioned_model"], expect_pass=True)
        result = run_dbt(["run", "--select", "incremental_partitioned_model"], expect_pass=True)
        partition_columns, _ = get_partition_columns_with_transforms(
            project, result.results[0].node
        )
        assert partition_columns == ["ds", "region",]

    def test_incremental_partition_by_full_refresh_sets_partition_columns(self, project):
        result = run_dbt(
            ["run", "--select", "incremental_partitioned_model", "--full-refresh"],
            expect_pass=True,
        )
        partition_columns, _ = get_partition_columns_with_transforms(
            project, result.results[0].node
        )
        assert partition_columns == ["ds", "region",]

    def test_python_partitioned_by_sets_partition_columns(self, project):
        result = run_dbt(["run", "--select", "python_partitioned_model"], expect_pass=True)
        partition_columns, _ = get_partition_columns_with_transforms(
            project, result.results[0].node
        )
        assert partition_columns == ["ds"]

    def test_table_partitioned_by_transform_sets_partition_columns(self, project):
        result = run_dbt(["run", "--select", "transform_partitioned_model"], expect_pass=True)
        partition_columns, transforms = get_partition_columns_with_transforms(
            project, result.results[0].node
        )
        assert partition_columns == ["ts", "region"]
        assert "day" in transforms[0]
        assert transforms[1] in ("identity", "none", "")


@pytest.mark.skip_profile("buenavista")
@pytest.mark.skip_database_type(
    "ducklake", reason="This test validates behavior on a non-DuckLake database"
)
class TestNonDucklakePartitionedBy:
    @pytest.fixture(scope="class")
    def models(self, models__non_ducklake_partitioned_table):
        return {
            "non_ducklake_partitioned_table.sql": models__non_ducklake_partitioned_table,
        }

    def test_partitioned_by_is_ignored_for_non_ducklake(self, project):
        run_dbt(["run", "--select", "non_ducklake_partitioned_table"], expect_pass=True)
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="non_ducklake_partitioned_table",
        )
        row_count = project.run_sql(f"select count(*) from {relation}", fetch="one")[0]
        assert row_count == 2


class TestPartitionedByValidation(BaseDucklakeIntegration):
    @pytest.fixture(scope="class")
    def models(self, models__invalid_partitioned_by, models__empty_partitioned_by_list):
        return {
            "invalid_partitioned_by.sql": models__invalid_partitioned_by,
            "empty_partitioned_by_list.sql": models__empty_partitioned_by_list,
        }

    def test_partitioned_by_list_values_must_be_strings(self, project):
        result = run_dbt(["run", "--select", "invalid_partitioned_by"], expect_pass=False)
        assert "partitioned_by/partition_by list values must be non-empty strings" in str(
            result.results[0].message
        )

    def test_partitioned_by_empty_list_is_invalid(self, project):
        result = run_dbt(["run", "--select", "empty_partitioned_by_list"], expect_pass=False)
        assert "partitioned_by/partition_by must contain at least one column" in str(
            result.results[0].message
        )
