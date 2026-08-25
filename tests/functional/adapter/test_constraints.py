import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
)
from dbt.tests.util import run_dbt


pytestmark = pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support primary or unique constraints"
)


class DuckDBColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "TIMESTAMP", "TIMESTAMP"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"],
            ["ARRAY['a','b','c']", "VARCHAR[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "INTEGER[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                "json",
                "JSON",
            ],
        ]


class TestTableConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass


@pytest.mark.skip_profile("md")
class TestTableConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement
):
    pass


@pytest.mark.skip_profile("md", "buenavista")
class TestTableConstraintsRollback(DuckDBColumnEqualSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md")
class TestIncrementalConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md", "buenavista")
class TestIncrementalConstraintsRollback(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRollback
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md")
class TestModelConstraintsRuntimeEnforcement(
    DuckDBColumnEqualSetup, BaseModelConstraintsRuntimeEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md", "buenavista")
class TestCompositeModelLevelForeignKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": "select 1 as id, 'parent' as type",
            "child.sql": "select 1 as parent_id, 'parent' as parent_type",
            "schema.yml": """
version: 2
models:
  - name: parent
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, type]
    columns:
      - name: id
        data_type: integer
      - name: type
        data_type: varchar
  - name: child
    config:
      contract:
        enforced: true
    constraints:
      - type: foreign_key
        columns: [parent_id, parent_type]
        to: ref('parent')
        to_columns: [id, type]
    columns:
      - name: parent_id
        data_type: integer
      - name: parent_type
        data_type: varchar
""",
        }

    def test_composite_model_level_foreign_key(self, project):
        results = run_dbt(["run"])

        assert len(results) == 2
