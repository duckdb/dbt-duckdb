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

# Test cases for is_struct method
@pytest.mark.parametrize("dtype, expected", [
    ("struct(a integer, b varchar)", True),
    ("struct(a integer)", True),
    ("STRUCT(a integer, b varchar)", True),
    ("integer", False),
    ("varchar", False),
])
def test_is_struct(dtype, expected):
    column = DuckDBColumn(column="struct_test", dtype=dtype)
    assert column.is_struct() == expected

# Test cases for flatten method
def test_flatten_simple_struct():
    column = DuckDBColumn(column="struct_test", dtype="struct(a integer, b varchar)")
    flattened = column.flatten()
    assert len(flattened) == 2
    assert flattened[0].column == "struct_test.a"
    assert flattened[0].dtype == "integer"
    assert flattened[1].column == "struct_test.b"
    assert flattened[1].dtype == "varchar"

def test_flatten_nested_struct():
    column = DuckDBColumn(column="struct_test", dtype="struct(a integer, b struct(c integer, d varchar))")
    flattened = column.flatten()
    assert len(flattened) == 3
    assert flattened[0].column == "struct_test.a"
    assert flattened[0].dtype == "integer"
    assert flattened[1].column == "struct_test.b.c"
    assert flattened[1].dtype == "integer"
    assert flattened[2].column == "struct_test.b.d"
    assert flattened[2].dtype == "varchar"

def test_flatten_non_struct():
    column = DuckDBColumn(column="integer_test", dtype="integer")
    flattened = column.flatten()
    assert len(flattened) == 1
    assert flattened[0].column == "integer_test"
    assert flattened[0].dtype == "integer"