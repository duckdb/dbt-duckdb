import pytest

from dbt.tests.util import (
    get_connection,
    relation_from_name,
    run_dbt,
)

# duckdb__get_columns_in_relation resolves columns via DESCRIBE (issue #762). Pin the
# type-string fidelity (precision + nested types) and the missing-relation -> [] guard.

types_model_sql = """
{{ config(materialized='table') }}

select
    1::integer as id,
    1.5::decimal(18, 2) as amount,
    'x'::varchar as label,
    cast(null as struct(fname varchar, age integer)) as nested
"""


class TestGetColumnsInRelation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"types_model.sql": types_model_sql}

    def test_describe_preserves_type_strings(self, project):
        run_dbt(["run"])

        relation = relation_from_name(project.adapter, "types_model")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(relation)

        dtypes = {c.name: c.dtype for c in columns}
        assert dtypes["id"] == "INTEGER"
        # Precision/scale are carried in the type string, not separate fields.
        assert dtypes["amount"] == "DECIMAL(18,2)"
        assert dtypes["label"] == "VARCHAR"
        assert dtypes["nested"] == "STRUCT(fname VARCHAR, age INTEGER)"

    def test_missing_relation_returns_empty_list(self, project):
        relation = relation_from_name(project.adapter, "does_not_exist_xyz")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(relation)
        assert columns == []
