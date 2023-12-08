import pytest

from dbt.adapters.duckdb.column import DuckDBColumn

# Test cases for is_float method
@pytest.mark.parametrize("dtype, expected", [
    ("real", True),
    ("float", True),
    ("float4", True),
    ("float8", True),
    ("double", True),
    ("integer", False),
    ("string", False),
    ("bigint", False)
])
def test_is_float(dtype, expected):
    column = DuckDBColumn(column="float_test", dtype=dtype)
    assert column.is_float() == expected

# Test cases for is_integer method
@pytest.mark.parametrize("dtype, expected", [
    ("tinyint", True),
    ("smallint", True),
    ("integer", True),
    ("bigint", True),
    ("hugeint", True),
    ("utinyint", True),
    ("usmallint", True),
    ("uinteger", True),
    ("ubigint", True),
    ("int1", True),
    ("int2", True),
    ("int4", True),
    ("int8", True),
    ("short", True),
    ("int", True),
    ("signed", True),
    ("long", True),
    ("float", False),
    ("string", False),
    ("double", False)
])
def test_is_integer(dtype, expected):
    column = DuckDBColumn(column="integer_test", dtype=dtype)
    assert column.is_integer() == expected
