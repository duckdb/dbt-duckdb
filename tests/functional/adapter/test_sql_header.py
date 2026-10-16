import pytest

from dbt.tests.util import run_dbt

model_sql = """
{{ config(materialized='table') }}
{% call set_sql_header(config) %}
create or replace temp macro magic() as 42;
{% endcall %}
select magic() as answer
"""

contract_schema_yml = """
models:
  - name: header_model
    config:
      contract:
        enforced: true
    columns:
      - name: answer
        data_type: integer
"""


class BaseSqlHeader:
    def test_sql_header(self, project):
        run_dbt(["run"])
        rows = project.run_sql("select answer from {schema}.header_model", fetch="one")
        assert rows[0] == 42


class TestSqlHeader(BaseSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {"header_model.sql": model_sql}


class TestSqlHeaderWithContract(BaseSqlHeader):
    """sql_header used to break enforced contracts: the header was inlined into the
    DESCRIBE the schema check runs (issue #515)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"header_model.sql": model_sql, "schema.yml": contract_schema_yml}
