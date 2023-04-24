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


class DuckDBColumnEqualSetup:
    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "bool"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "DATETIME"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "DATETIME"],
            ["ARRAY['a','b','c']", "text[]", "STRINGARRAY"],
            ["ARRAY[1,2,3]", "int[]", "INTEGERARRAY"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            ["""{'bar': 'baz', 'balance': 7.77, 'active': false}""", "struct(bar text, balance decimal, active boolean)", "json"],
        ]


class TestTableConstraintsColumnsEqual(DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqual(DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqual(DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    pass


class TestTableConstraintsRuntimeDdlEnforcement(DuckDBColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement):
    pass


class TestTableConstraintsRollback(DuckDBColumnEqualSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


class TestIncrementalConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


class TestIncrementalConstraintsRollback(DuckDBColumnEqualSetup, BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]
    
class TestModelConstraintsRuntimeEnforcement(DuckDBColumnEqualSetup, BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]
