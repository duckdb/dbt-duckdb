import pytest
from dbt.tests.util import run_dbt

from tests.ducklake import BaseDucklakeIntegration
from tests.ducklake import ducklake_metadata_schema


@pytest.fixture(scope="class")
def models__table_sorted_model(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', sorted_by='ds') }}}}

select 1 as id, '2025-01-01' as ds, 'us' as region, 10 as amount
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region, 20 as amount
"""


@pytest.fixture(scope="class")
def models__incremental_sorted_model(test_database_name):
    return f"""
{{{{ config(
    materialized='incremental',
    database='{test_database_name}',
    unique_key='id',
    sort_by=['ds', 'region']
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
def models__python_sorted_model(test_database_name):
    return f"""
import pandas as pd


def model(dbt, _):
    dbt.config(materialized='table', database='{test_database_name}', sorted_by='ds')
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
def models__partitioned_and_sorted_model(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', partitioned_by='ds', sorted_by='region') }}}}

select 1 as id, '2025-01-01' as ds, 'us' as region, 10 as amount
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region, 20 as amount
"""


@pytest.fixture(scope="class")
def models__non_ducklake_sorted_table():
    return """
{{ config(materialized='table', sorted_by='ds') }}

select 1 as ds, 'a' as value
union all
select 2 as ds, 'b' as value
"""


@pytest.fixture(scope="class")
def models__invalid_sorted_by(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', sorted_by=['ds', 1]) }}}}

select 1 as ds, 'a' as value
"""


@pytest.fixture(scope="class")
def models__empty_sorted_by_list(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', sorted_by=[]) }}}}

select 1 as ds, 'a' as value
"""


@pytest.fixture(scope="class")
def models__invalid_sorted_by_string(test_database_name):
    return f"""
{{{{ config(materialized='table', database='{test_database_name}', sorted_by='ds); drop table x; --') }}}}

select 1 as ds, 'a' as value
"""


def get_sort_columns(project, node):
    metadata_schema = ducklake_metadata_schema(node.database)
    query = f"""
        select se.expression
        from {metadata_schema}.ducklake_sort_info si
        join {metadata_schema}.ducklake_sort_expression se
          on se.sort_id = si.sort_id
         and se.table_id = si.table_id
        join {metadata_schema}.ducklake_table t
          on t.table_id = si.table_id
        join {metadata_schema}.ducklake_schema s
          on s.schema_id = t.schema_id
        where lower(t.table_name) = lower('{node.alias}')
          and lower(s.schema_name) = lower('{node.schema}')
          and t.end_snapshot is null
          and s.end_snapshot is null
          and si.end_snapshot is null
        order by se.sort_key_index
    """
    return [row[0].lower() for row in project.run_sql(query, fetch="all")]


class TestDucklakeSortedByIntegration(BaseDucklakeIntegration):
    @pytest.fixture(scope="class")
    def models(
        self,
        models__table_sorted_model,
        models__incremental_sorted_model,
        models__python_sorted_model,
        models__partitioned_and_sorted_model,
    ):
        return {
            "table_sorted_model.sql": models__table_sorted_model,
            "incremental_sorted_model.sql": models__incremental_sorted_model,
            "python_sorted_model.py": models__python_sorted_model,
            "partitioned_and_sorted_model.sql": models__partitioned_and_sorted_model,
        }

    def test_table_sorted_by_sets_sort_columns(self, project):
        result = run_dbt(["run", "--select", "table_sorted_model"], expect_pass=True)
        assert get_sort_columns(project, result.results[0].node) == ["ds"]

    def test_table_sorted_by_is_idempotent(self, project):
        run_dbt(["run", "--select", "table_sorted_model"], expect_pass=True)
        result = run_dbt(["run", "--select", "table_sorted_model"], expect_pass=True)
        assert get_sort_columns(project, result.results[0].node) == ["ds"]

    def test_incremental_sort_by_sets_sort_columns(self, project):
        run_dbt(["run", "--select", "incremental_sorted_model"], expect_pass=True)
        result = run_dbt(["run", "--select", "incremental_sorted_model"], expect_pass=True)
        assert get_sort_columns(project, result.results[0].node) == ["ds", "region"]

    def test_incremental_sort_by_full_refresh_sets_sort_columns(self, project):
        result = run_dbt(
            ["run", "--select", "incremental_sorted_model", "--full-refresh"],
            expect_pass=True,
        )
        assert get_sort_columns(project, result.results[0].node) == ["ds", "region"]

    def test_python_sorted_by_sets_sort_columns(self, project):
        result = run_dbt(["run", "--select", "python_sorted_model"], expect_pass=True)
        assert get_sort_columns(project, result.results[0].node) == ["ds"]

    def test_partitioned_and_sorted_together(self, project):
        result = run_dbt(["run", "--select", "partitioned_and_sorted_model"], expect_pass=True)
        assert get_sort_columns(project, result.results[0].node) == ["region"]


@pytest.mark.skip_profile("buenavista")
@pytest.mark.skip_database_type(
    "ducklake", reason="This test validates behavior on a non-DuckLake database"
)
class TestNonDucklakeSortedBy:
    @pytest.fixture(scope="class")
    def models(self, models__non_ducklake_sorted_table):
        return {
            "non_ducklake_sorted_table.sql": models__non_ducklake_sorted_table,
        }

    def test_sorted_by_is_ignored_for_non_ducklake(self, project):
        run_dbt(["run", "--select", "non_ducklake_sorted_table"], expect_pass=True)
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="non_ducklake_sorted_table",
        )
        row_count = project.run_sql(f"select count(*) from {relation}", fetch="one")[0]
        assert row_count == 2


class TestSortedByValidation(BaseDucklakeIntegration):
    @pytest.fixture(scope="class")
    def models(
        self,
        models__invalid_sorted_by,
        models__empty_sorted_by_list,
        models__invalid_sorted_by_string,
    ):
        return {
            "invalid_sorted_by.sql": models__invalid_sorted_by,
            "empty_sorted_by_list.sql": models__empty_sorted_by_list,
            "invalid_sorted_by_string.sql": models__invalid_sorted_by_string,
        }

    def test_sorted_by_list_values_must_be_strings(self, project):
        result = run_dbt(["run", "--select", "invalid_sorted_by"], expect_pass=False)
        assert "sorted_by/sort_by list values must be non-empty strings" in str(
            result.results[0].message
        )

    def test_sorted_by_empty_list_is_invalid(self, project):
        result = run_dbt(["run", "--select", "empty_sorted_by_list"], expect_pass=False)
        assert "sorted_by/sort_by must contain at least one column" in str(
            result.results[0].message
        )

    def test_sorted_by_invalid_string_rejected_by_duckdb(self, project):
        """Invalid identifier is quoted and DuckDB rejects it as a nonexistent column."""
        result = run_dbt(["run", "--select", "invalid_sorted_by_string"], expect_pass=False)
        message = str(result.results[0].message)
        assert "not found" in message or "does not exist" in message
