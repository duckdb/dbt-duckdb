import re
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt


# NOTE: This module intentionally includes "compile-only" tests that call low-level adapter
# macros (e.g. `duckdb__create_table_as` / `py_write_table`) via small helper macros.
#
# Rationale:
# - The DuckLake `sorted_by` implementation is mostly Jinja logic that decides whether to
#   emit `ALTER TABLE .. SET SORTED BY ..` and how to validate/parse user input.
# - These tests unit-test that compilation output includes (or omits) the expected statements,
#   without requiring DuckLake attachments or DuckLake metadata tables.
#
# See `tests/functional/adapter/test_ducklake_sorted_by_integration.py` for end-to-end
# integration tests that assert sort metadata on real DuckLake tables.

models__sorted_by_table = """
{{ config(materialized='view', sorted_by='ds') }}

{{ render_create_table_as() }}
"""

models__sort_by_incremental = """
{{ config(materialized='view', sort_by=['ds', 'region']) }}

{{ render_create_table_as() }}
"""

models__sorted_by_python = """
{{ config(materialized='view', sorted_by='ds') }}

{{ render_py_write_table() }}
"""

models__contract_sorted_by = """
{{ config(materialized='view', sorted_by='ds', contract={'enforced': True}) }}

{{ render_create_table_as("select 1 as ds, 'a' as value") }}
"""

models__partitioned_and_sorted = """
{{ config(materialized='view', partitioned_by='ds', sorted_by='region') }}

{{ render_create_table_as("select 1 as ds, 'a' as region") }}
"""

models__invalid_sorted_by = """
{{ config(materialized='view', sorted_by=['ds', 1]) }}

{{ render_create_table_as() }}
"""


schema_yml = """
version: 2

models:
  - name: contract_sorted_by
    columns:
      - name: ds
        data_type: integer
      - name: value
        data_type: string
"""

macros__render_create_table_as = """
{% macro render_create_table_as(compiled_code='select 1 as ds', temporary=false, language='sql') %}
  {%- set partitioned_by = duckdb__get_partitioned_by(this, temporary) -%}
  {%- set sorted_by = duckdb__get_sorted_by(this, temporary) -%}
  {{ return(duckdb__create_table_as(temporary, this, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by)) }}
{% endmacro %}
"""

macros__render_py_write_table = """
{% macro render_py_write_table(compiled_code='def model(dbt, session):\\n    return None', temporary=false) %}
  {%- set partitioned_by = duckdb__get_partitioned_by(this, temporary) -%}
  {%- set sorted_by = duckdb__get_sorted_by(this, temporary) -%}
  {{ return(py_write_table(temporary, this, compiled_code, partitioned_by=partitioned_by, sorted_by=sorted_by)) }}
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


class BaseSortedByCompile:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ducklake_sorted_by",
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sorted_by_table.sql": models__sorted_by_table,
            "sort_by_incremental.sql": models__sort_by_incremental,
            "sorted_by_python.sql": models__sorted_by_python,
            "contract_sorted_by.sql": models__contract_sorted_by,
            "partitioned_and_sorted.sql": models__partitioned_and_sorted,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "render_create_table_as.sql": macros__render_create_table_as,
            "render_py_write_table.sql": macros__render_py_write_table,
        }


class TestDucklakeSortedByCompile(BaseSortedByCompile):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        # Avoid mutating the session-scoped fixture in-place.
        target = dict(dbt_profile_target)
        target["is_ducklake"] = True
        return target

    def test_sorted_by_string(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "sorted_by_table", "sql")
        assert re.search(r'alter\s+table.*set\s+sorted\s+by\s*\(\s*"ds"\s*\)', sql, re.IGNORECASE | re.DOTALL)

    def test_sort_by_list(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "sort_by_incremental", "sql")
        assert re.search(
            r'alter\s+table.*set\s+sorted\s+by\s*\(\s*"ds"\s*,\s*"region"\s*\)',
            sql,
            re.IGNORECASE | re.DOTALL,
        )

    def test_sorted_by_python(self, project):
        run_dbt(["compile"])
        python_code = read_compiled_file(project, "sorted_by_python", "sql")
        assert re.search(
            r'alter\s+table.*set\s+sorted\s+by\s*\(\s*"ds"\s*\)',
            python_code,
            re.IGNORECASE | re.DOTALL,
        )

    def test_sorted_by_contract(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "contract_sorted_by", "sql")
        assert re.search(r'alter\s+table.*set\s+sorted\s+by\s*\(\s*"ds"\s*\)', sql, re.IGNORECASE | re.DOTALL)

    def test_partitioned_and_sorted_together(self, project):
        run_dbt(["compile"])
        sql = read_compiled_file(project, "partitioned_and_sorted", "sql")
        # Both ALTERs should appear, with partitioned before sorted.
        assert re.search(
            r'alter\s+table.*set\s+partitioned\s+by\s*\(\s*"ds"\s*\).*'
            r'alter\s+table.*set\s+sorted\s+by\s*\(\s*"region"\s*\)',
            sql,
            re.IGNORECASE | re.DOTALL,
        )


@pytest.mark.skip_database_type(
    "ducklake", reason="This test validates behavior on a non-DuckLake database"
)
class TestNonDucklakeSortedByCompile(BaseSortedByCompile):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        # Avoid mutating the session-scoped fixture in-place.
        target = dict(dbt_profile_target)
        target.pop("is_ducklake", None)
        return target

    def test_sorted_by_ignored(self, project):
        run_dbt(["compile"])
        sql_table = read_compiled_file(project, "sorted_by_table", "sql").lower()
        sql_incremental = read_compiled_file(project, "sort_by_incremental", "sql").lower()
        sql_contract = read_compiled_file(project, "contract_sorted_by", "sql").lower()
        python_code = read_compiled_file(project, "sorted_by_python", "sql").lower()
        assert "set sorted by" not in sql_table
        assert "set sorted by" not in sql_incremental
        assert "set sorted by" not in sql_contract
        assert "set sorted by" not in python_code


class TestSortedByValidation:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ducklake_sorted_by_validation",
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_sorted_by.sql": models__invalid_sorted_by,
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

    def test_sorted_by_list_values_must_be_strings(self, project):
        with pytest.raises(Exception, match="sorted_by/sort_by list values must be non-empty strings"):
            run_dbt(["compile"])
