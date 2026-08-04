import duckdb
"A bit verbose, but this is one of the main areas where the accepted syntax for DuckDB databases and DuckLake differ."

import pytest

from dbt.tests.util import get_connection, relation_from_name, run_dbt


table_model_sql = """
{{ config(materialized='table') }}

select 1 as id
"""

pytestmark = pytest.mark.skip_profile("buenavista")


# TODO: Add equivalent trigger dependency coverage once DuckDB 2.0 is out
def drop_table_model(project):
    table_address = relation_from_name(project.adapter, "table_model")

    with get_connection(project.adapter):
        table = project.adapter.get_relation(
            database=table_address.database,
            schema=table_address.schema,
            identifier=table_address.identifier,
        )
        assert table is not None

        project.adapter.drop_relation(table)

        assert project.adapter.get_relation(
            database=table.database,
            schema=table.schema,
            identifier=table.identifier,
        ) is None


def assert_drop_without_cascade_fails(project):
    table = relation_from_name(project.adapter, "table_model")

    with pytest.raises(duckdb.DependencyException, match="depends on"):
        project.run_sql(f"drop table {table}")


class TestDropRelation:
    @pytest.fixture(scope="class")
    @classmethod
    def models(cls):
        return {"table_model.sql": table_model_sql}

    def test_drop_table(self, project):
        run_dbt(["run"])
        drop_table_model(project)


@pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support relation-level cascade drops"
)
class TestDropRelationWithViewDependencies:
    @pytest.fixture(scope="class")
    @classmethod
    def dbt_profile_target(cls, dbt_profile_target):
        return {
            **dbt_profile_target,
            "settings": {
                **dbt_profile_target.get("settings", {}),
                "enable_view_dependencies": True,
            },
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(cls):
        return {"table_model.sql": table_model_sql}

    def test_drop_table_cascades_to_dependent_view(self, project):
        run_dbt(["run"])
        table = relation_from_name(project.adapter, "table_model")
        view_address = relation_from_name(project.adapter, "dependent_view")
        project.run_sql(f"create view {view_address} as select * from {table}")

        assert_drop_without_cascade_fails(project)
        drop_table_model(project)

        with get_connection(project.adapter):
            assert project.adapter.get_relation(
                database=view_address.database,
                schema=view_address.schema,
                identifier=view_address.identifier,
            ) is None


@pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support relation-level cascade drops"
)
class TestDropRelationWithMacroDependencies:
    @pytest.fixture(scope="class")
    @classmethod
    def dbt_profile_target(cls, dbt_profile_target):
        return {
            **dbt_profile_target,
            "settings": {
                **dbt_profile_target.get("settings", {}),
                "enable_macro_dependencies": True,
            },
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(cls):
        return {"table_model.sql": table_model_sql}

    def test_drop_table_cascades_to_dependent_macro(self, project):
        run_dbt(["run"])
        table = relation_from_name(project.adapter, "table_model")
        macro = relation_from_name(project.adapter, "dependent_table_macro")
        project.run_sql(f"create macro {macro}() as table select * from {table}")

        assert_drop_without_cascade_fails(project)
        drop_table_model(project)

        macro_count = project.run_sql(
            f"""
            select count(*)
            from duckdb_functions()
            where database_name = '{macro.database}'
              and schema_name = '{macro.schema}'
              and function_name = '{macro.identifier}'
            """,
            fetch="one",
        )[0]
        assert macro_count == 0
