from pathlib import Path

import pytest
from dbt.tests.util import run_dbt


models__table_partitioned_model = """
{{ config(materialized='table', database='ducklake_db', partitioned_by='ds') }}

select 1 as id, '2025-01-01' as ds, 'us' as region, 10 as amount
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region, 20 as amount
"""

models__incremental_partitioned_model = """
{{ config(
    materialized='incremental',
    database='ducklake_db',
    unique_key='id',
    partition_by=['ds', 'region']
) }}

{% if is_incremental() %}
select 2 as id, '2025-01-02' as ds, 'eu' as region, 22 as amount
union all
select 3 as id, '2025-01-03' as ds, 'ca' as region, 30 as amount
{% else %}
select 1 as id, '2025-01-01' as ds, 'us' as region, 10 as amount
union all
select 2 as id, '2025-01-02' as ds, 'eu' as region, 20 as amount
{% endif %}
"""

models__python_partitioned_model = """
import pandas as pd


def model(dbt, _):
    dbt.config(materialized='table', database='ducklake_db', partitioned_by='ds')
    return pd.DataFrame(
        {
            "id": [1, 2],
            "ds": ["2025-01-01", "2025-01-02"],
            "region": ["us", "eu"],
            "amount": [10, 20],
        }
    )
"""

models__non_ducklake_partitioned_table = """
{{ config(materialized='table', partitioned_by='ds') }}

select 1 as ds, 'a' as value
union all
select 2 as ds, 'b' as value
"""

models__invalid_partitioned_by = """
{{ config(materialized='table', database='ducklake_db', partitioned_by=['ds', 1]) }}

select 1 as ds, 'a' as value
"""

models__empty_partitioned_by_list = """
{{ config(materialized='table', database='ducklake_db', partitioned_by=[]) }}

select 1 as ds, 'a' as value
"""

models__invalid_partitioned_by_string = """
{{ config(materialized='table', database='ducklake_db', partitioned_by='ds); drop table x; --') }}

select 1 as ds, 'a' as value
"""


def get_partition_columns(project, model_name, schema_name):
    relation = project.adapter.Relation.create(
        database="ducklake_db",
        schema=schema_name,
        identifier=model_name,
    )
    metadata_schema = "__ducklake_metadata_ducklake_db"
    query = f"""
        select c.column_name
        from {metadata_schema}.ducklake_partition_column pc
        join {metadata_schema}.ducklake_column c
          on c.table_id = pc.table_id
         and c.column_id = pc.column_id
        join {metadata_schema}.ducklake_table t
          on t.table_id = c.table_id
        join {metadata_schema}.ducklake_schema s
          on s.schema_id = t.schema_id
        where lower(t.table_name) = lower('{relation.identifier}')
          and lower(s.schema_name) = lower('{relation.schema}')
          and t.end_snapshot is null
          and c.end_snapshot is null
          and s.end_snapshot is null
        order by c.column_id
    """
    return [row[0].lower() for row in project.run_sql(query, fetch="all")]


@pytest.mark.skip_profile("buenavista", "md")
class BaseDucklakePartitionedBy:
    @pytest.fixture(scope="class")
    def ducklake_attachment(self, tmp_path_factory):
        root = Path(tmp_path_factory.mktemp("ducklake_partitioned_by"))
        metadata_path = root / "metadata.ducklake"
        data_path = root / "data"
        data_path.mkdir(parents=True, exist_ok=True)

        return {
            "path": f"ducklake:sqlite:{metadata_path}",
            "alias": "ducklake_db",
            "options": {"data_path": str(data_path)},
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, ducklake_attachment):
        target = dict(dbt_profile_target)
        target["path"] = target.get("path", ":memory:")
        target["attach"] = [ducklake_attachment]
        return {
            "test": {
                "outputs": {"dev": target},
                "target": "dev",
            }
        }


class TestDucklakePartitionedByIntegration(BaseDucklakePartitionedBy):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_partitioned_model.sql": models__table_partitioned_model,
            "incremental_partitioned_model.sql": models__incremental_partitioned_model,
            "python_partitioned_model.py": models__python_partitioned_model,
        }

    def test_table_partitioned_by_sets_partition_columns(self, project):
        result = run_dbt(["run", "--select", "table_partitioned_model"], expect_pass=True)
        schema = result.results[0].node.schema
        assert get_partition_columns(project, "table_partitioned_model", schema) == ["ds"]

    def test_table_partitioned_by_is_idempotent(self, project):
        run_dbt(["run", "--select", "table_partitioned_model"], expect_pass=True)
        result = run_dbt(["run", "--select", "table_partitioned_model"], expect_pass=True)
        schema = result.results[0].node.schema
        assert get_partition_columns(project, "table_partitioned_model", schema) == ["ds"]

    def test_incremental_partition_by_sets_partition_columns(self, project):
        run_dbt(["run", "--select", "incremental_partitioned_model"], expect_pass=True)
        result = run_dbt(["run", "--select", "incremental_partitioned_model"], expect_pass=True)
        schema = result.results[0].node.schema
        assert get_partition_columns(project, "incremental_partitioned_model", schema) == ["ds", "region"]

    def test_incremental_partition_by_full_refresh_sets_partition_columns(self, project):
        result = run_dbt(
            ["run", "--select", "incremental_partitioned_model", "--full-refresh"],
            expect_pass=True,
        )
        schema = result.results[0].node.schema
        assert get_partition_columns(project, "incremental_partitioned_model", schema) == ["ds", "region"]

    def test_python_partitioned_by_sets_partition_columns(self, project):
        result = run_dbt(["run", "--select", "python_partitioned_model"], expect_pass=True)
        schema = result.results[0].node.schema
        assert get_partition_columns(project, "python_partitioned_model", schema) == ["ds"]


@pytest.mark.skip_profile("buenavista")
class TestNonDucklakePartitionedBy:
    @pytest.fixture(scope="class")
    def models(self):
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


class TestPartitionedByValidation(BaseDucklakePartitionedBy):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_partitioned_by.sql": models__invalid_partitioned_by,
            "empty_partitioned_by_list.sql": models__empty_partitioned_by_list,
            "invalid_partitioned_by_string.sql": models__invalid_partitioned_by_string,
        }

    def test_partitioned_by_list_values_must_be_strings(self, project):
        result = run_dbt(["run", "--select", "invalid_partitioned_by"], expect_pass=False)
        assert (
            "partitioned_by/partition_by list values must be non-empty strings"
            in str(result.results[0].message)
        )

    def test_partitioned_by_empty_list_is_invalid(self, project):
        result = run_dbt(["run", "--select", "empty_partitioned_by_list"], expect_pass=False)
        assert "partitioned_by/partition_by list must contain at least one value" in str(
            result.results[0].message
        )

    def test_partitioned_by_string_must_be_identifier_list(self, project):
        result = run_dbt(["run", "--select", "invalid_partitioned_by_string"], expect_pass=False)
        assert "partitioned_by/partition_by values must be valid column identifiers" in str(
            result.results[0].message
        )

