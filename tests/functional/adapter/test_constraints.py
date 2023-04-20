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
    def int_type(self):
        return "NUMBER"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"
        
    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            # DuckDB's cursor doesn't seem to distinguish between:
            #   INT and NUMERIC/DECIMAL -- both just return as 'NUMBER'
            #   TIMESTAMP and TIMESTAMPTZ -- both just return as 'DATETIME'
            #   [1,2,3] and ['a','b','c'] -- both just return as 'list'
            
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "bool"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "DATETIME"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "DATETIME"],
            ["ARRAY['a','b','c']", "text[]", "list"],
            ["ARRAY[1,2,3]", "int[]", "list"],
            #["'1'::numeric", "numeric", "DECIMAL"],    -- no distinction, noted above
            ["""{'bar': 'baz', 'balance': 7.77, 'active': false}""", "struct(bar text, balance decimal, active boolean)", "dict"],
        ]


class TestTableConstraintsColumnsEqual(DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqual(DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqual(DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    pass


class TestTableConstraintsRuntimeDdlEnforcement(DuckDBColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement):
    pass


class TestTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


class TestIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


class TestIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]
    
class TestModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]
