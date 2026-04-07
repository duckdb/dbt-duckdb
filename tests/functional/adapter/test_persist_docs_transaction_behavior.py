import uuid

import pytest
from dbt.tests.util import relation_from_name, run_dbt


audit_table = f"persist_docs_audit_{uuid.uuid4().hex}"


view_model_sql = """
{{ config(materialized='view') }}
select 1 as id, 'Joe' as name
"""


schema_yml = """
version: 2

models:
  - name: view_model
    description: "View model description"
    columns:
      - name: id
        description: "id Column description"
"""


persist_docs_macros = f"""
{{% macro duckdb__alter_relation_comment(relation, comment) %}}
  insert into {audit_table}
  values ('relation', {{{{ adapter.is_transaction_open() }}}})
{{% endmacro %}}

{{% macro duckdb__alter_column_comment(relation, column_dict) %}}
  insert into {audit_table}
  values ('column', {{{{ adapter.is_transaction_open() }}}})
{{% endmacro %}}

{{% macro duckdb_get_column_comment_sql(relation, column_name, column_config) %}}
  insert into {audit_table}
  values ('column', {{{{ adapter.is_transaction_open() }}}})
{{% endmacro %}}
"""


@pytest.mark.skip_profile("md")
class TestPersistDocsTransactionBehaviorView:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                        "transaction": False,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"view_model.sql": view_model_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": schema_yml}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"persist_docs_transaction_macros.sql": persist_docs_macros}

    def test_view_persist_docs_runs_outside_transaction(self, project):
        project.run_sql(f"create table {audit_table} (kind varchar, transaction_open boolean)")

        run_dbt(["run"])

        relation = relation_from_name(project.adapter, "view_model")
        assert relation is not None

        results = project.run_sql(
            f"select kind, transaction_open from {audit_table} order by kind", fetch="all"
        )

        assert results == [("column", False), ("relation", False)]
