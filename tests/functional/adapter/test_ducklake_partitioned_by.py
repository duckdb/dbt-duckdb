import re
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt


# NOTE: This module intentionally includes "compile-only" tests that call low-level adapter
# macros (e.g. `duckdb__create_table_as` / `py_write_table`) via small helper macros.
#
# Rationale:
# - The DuckLake `partitioned_by` implementation is mostly Jinja logic that decides whether to
#   emit `ALTER TABLE .. SET PARTITIONED BY ..` and how to validate/parse user input.
# - These tests unit-test that compilation output includes (or omits) the expected statements,
#   without requiring DuckLake attachments or DuckLake metadata tables.
#
# See `tests/functional/adapter/test_ducklake_partitioned_by_integration.py` for end-to-end
# integration tests that assert partition metadata on real DuckLake tables.

models__partitioned_by_table = """
{{ config(materialized='view', partitioned_by='ds') }}

{{ render_create_table_as() }}
"""

models__partition_by_incremental = """
{{ config(materialized='view', partition_by=['ds', 'region']) }}

{{ render_create_table_as() }}
"""

models__partitioned_by_time_parts = """
{{ config(materialized='view', partitioned_by=['event_day', 'event_month', 'event_year', 'event_hour']) }}

{{ render_create_table_as("select 1 as event_day, 2 as event_month, 3 as event_year, 4 as event_hour") }}
"""

models__partitioned_by_python = """
{{ config(materialized='view', partitioned_by='ds') }}

{{ render_py_write_table() }}
"""

models__contract_partitioned_by = """
{{ config(materialized='view', partitioned_by='ds', contract={'enforced': True}) }}

{{ render_create_table_as("select 1 as ds, 'a' as value") }}
"""

models__invalid_partitioned_by = """
{{ config(materialized='view', partitioned_by=['ds', 1]) }}

{{ render_create_table_as() }}
"""


schema_yml = """
version: 2

models:
  - name: contract_partitioned_by
    columns:
      - name: ds
        data_type: integer
      - name: value
        data_type: string
"""

macros__render_create_table_as = """
{% macro render_create_table_as(compiled_code='select 1 as ds', temporary=false, language='sql') %}
  {%- set partitioned_by = duckdb__get_partitioned_by(this, temporary) -%}
  {{ return(duckdb__create_table_as(temporary, this, compiled_code, language, partitioned_by=partitioned_by)) }}
{% endmacro %}
"""

macros__render_py_write_table = """
{% macro render_py_write_table(compiled_code='def model(dbt, session):\\n    return None', temporary=false) %}
  {%- set partitioned_by = duckdb__get_partitioned_by(this, temporary) -%}
  {{ return(py_write_table(temporary, this, compiled_code, partitioned_by=partitioned_by)) }}
{% endmacro %}
"""


def read_compiled_file(project, model_name, extension):
    compiled_root = Path(project.project_root) / "target" / "compiled"
    matches = list(compiled_root.rglob(f"{model_name}.{extension}"))
    assert matches, f"Compiled file not found for model '{model_name}.{extension}'"

    project_name = getattr(project, "project_name", None)
    if project_name:
        named_matches = [path for path in matches if project_name in path.parts]
        if named_matches:
            matches = named_matches

    assert len(matches) == 1, f"Expected one compiled file for '{model_name}', found {len(matches)}"
    return matches[0].read_text()


class BasePartitionedByCompile:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ducklake_partitioned_by",
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "partitioned_by_table.sql": models__partitioned_by_table,
            "partition_by_incremental.sql": models__partition_by_incremental,
            "partitioned_by_python.sql": models__partitioned_by_python,
            "contract_partitioned_by.sql": models__contract_partitioned_by,
            "partitioned_by_time_parts.sql": models__partitioned_by_time_parts,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "render_create_table_as.sql": macros__render_create_table_as,
            "render_py_write_table.sql": macros__render_py_write_table,
        }


class TestDucklakePartitionedByCompile(BasePartitionedByCompile):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        # Avoid mutating the session-scoped fixture in-place.
        target = dict(dbt_profile_target)
        target["is_ducklake"] = True
        return target

    def test_partitioned_by_string(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "partitioned_by_table", "sql")
        assert re.search(r"alter\s+table.*set\s+partitioned\s+by\s*\(\s*ds\s*\)", sql, re.IGNORECASE | re.DOTALL)

    def test_partition_by_list(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "partition_by_incremental", "sql")
        assert re.search(
            r"alter\s+table.*set\s+partitioned\s+by\s*\(\s*ds\s*,\s*region\s*\)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )

    def test_partitioned_by_python(self, project):
        run_dbt(["compile"])
        python_code = read_compiled_file(project, "partitioned_by_python", "sql")
        assert re.search(
            r"alter\s+table.*set\s+partitioned\s+by\s*\(\s*ds\s*\)",
            python_code,
            re.IGNORECASE | re.DOTALL,
        )

    def test_partitioned_by_contract(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "contract_partitioned_by", "sql")
        assert re.search(r"alter\s+table.*set\s+partitioned\s+by\s*\(\s*ds\s*\)", sql, re.IGNORECASE | re.DOTALL)

    def test_partitioned_by_time_parts(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "partitioned_by_time_parts", "sql")
        assert re.search(
            r"alter\s+table.*set\s+partitioned\s+by\s*\(\s*event_day\s*,\s*event_month\s*,\s*event_year\s*,\s*event_hour\s*\)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )


class TestNonDucklakePartitionedByCompile(BasePartitionedByCompile):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        # Avoid mutating the session-scoped fixture in-place.
        target = dict(dbt_profile_target)
        target.pop("is_ducklake", None)
        return target

    def test_partitioned_by_ignored(self, project):
        run_dbt(["compile"])
        sql_table = read_compiled_file(project, "partitioned_by_table", "sql").lower()
        sql_incremental = read_compiled_file(project, "partition_by_incremental", "sql").lower()
        sql_contract = read_compiled_file(project, "contract_partitioned_by", "sql").lower()
        sql_time_parts = read_compiled_file(project, "partitioned_by_time_parts", "sql").lower()
        python_code = read_compiled_file(project, "partitioned_by_python", "sql").lower()
        assert "set partitioned by" not in sql_table
        assert "set partitioned by" not in sql_incremental
        assert "set partitioned by" not in sql_contract
        assert "set partitioned by" not in sql_time_parts
        assert "set partitioned by" not in python_code


class TestPartitionedByValidation:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ducklake_partitioned_by_validation",
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_partitioned_by.sql": models__invalid_partitioned_by,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "render_create_table_as.sql": macros__render_create_table_as,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        # Avoid mutating the session-scoped fixture in-place.
        target = dict(dbt_profile_target)
        target["is_ducklake"] = True
        return target

    def test_partitioned_by_list_values_must_be_strings(self, project):
        with pytest.raises(Exception, match="partitioned_by/partition_by list values must be non-empty strings"):
            run_dbt(["compile"])
