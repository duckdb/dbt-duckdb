import pytest

from dbt.tests.util import get_connection, relation_from_name, run_dbt


table_model_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

dependent_view_sql = """
{{ config(materialized='view') }}

select * from {{ ref('table_model') }}
"""


@pytest.mark.skip_profile("buenavista")
class TestDropRelationDependencies:
    @pytest.fixture(scope="class")
    @classmethod
    def models(cls):
        return {
            "table_model.sql": table_model_sql,
            "dependent_view.sql": dependent_view_sql,
        }

    def test_view_does_not_prevent_or_cascade_with_table_drop(self, project):
        run_dbt(["run"])

        table_address = relation_from_name(project.adapter, "table_model")
        view_address = relation_from_name(project.adapter, "dependent_view")

        with get_connection(project.adapter):
            table = project.adapter.get_relation(
                database=table_address.database,
                schema=table_address.schema,
                identifier=table_address.identifier,
            )
            view = project.adapter.get_relation(
                database=view_address.database,
                schema=view_address.schema,
                identifier=view_address.identifier,
            )

            project.adapter.drop_relation(table)

            assert project.adapter.get_relation(
                database=table.database,
                schema=table.schema,
                identifier=table.identifier,
            ) is None
            assert project.adapter.get_relation(
                database=view.database,
                schema=view.schema,
                identifier=view.identifier,
            ) == view
