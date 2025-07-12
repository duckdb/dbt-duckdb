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
    column = DuckDBColumn.create(column="float_test", dtype=dtype)
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
    column = DuckDBColumn.create(column="integer_test", dtype=dtype)
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
    column = DuckDBColumn.create(column="struct_test", dtype=dtype)
    assert column.is_struct() == expected

# Test cases for flatten method
def test_flatten_simple_struct():
    fields = [
        DuckDBColumn.create(column="a", dtype="integer"),
        DuckDBColumn.create(column="b", dtype="varchar"),
    ]
    column = DuckDBColumn.create(column="struct_test", dtype="struct(a integer, b varchar)", fields=fields)
    flattened = column.flatten()
    assert len(flattened) == 2
    assert flattened[0].name == "struct_test.a"
    assert flattened[0].dtype == "integer"
    assert flattened[1].name == "struct_test.b"
    assert flattened[1].dtype == "varchar"

def test_flatten_nested_struct():
    nested_fields = [
        DuckDBColumn.create(column="c", dtype="integer"),
        DuckDBColumn.create(column="d", dtype="varchar"),
    ]
    fields = [
        DuckDBColumn.create(column="a", dtype="integer"),
        DuckDBColumn.create(column="b", dtype="struct(c integer, d varchar)", fields=nested_fields),
    ]
    column = DuckDBColumn.create(column="struct_test", dtype="struct(a integer, b struct(c integer, d varchar))", fields=fields)
    flattened = column.flatten()
    assert len(flattened) == 3
    assert flattened[0].name == "struct_test.a"
    assert flattened[0].dtype == "integer"
    assert flattened[1].name == "struct_test.b.c"
    assert flattened[1].dtype == "integer"
    assert flattened[2].name == "struct_test.b.d"
    assert flattened[2].dtype == "varchar"

def test_flatten_non_struct():
    column = DuckDBColumn.create(column="integer_test", dtype="integer")
    flattened = column.flatten()
    assert len(flattened) == 1
    assert flattened[0].name == "integer_test"
    assert flattened[0].dtype == "integer"

# Test cases for create method
def test_create_with_fields():
    fields = [
        DuckDBColumn.create(column="a", dtype="integer"),
        DuckDBColumn.create(column="b", dtype="varchar"),
    ]
    column = DuckDBColumn.create(column="struct_test", dtype="struct(a integer, b varchar)", fields=fields)
    assert column.name == "struct_test"
    assert column.dtype == "struct(a integer, b varchar)"
    assert len(column.fields) == 2
    assert column.fields[0].name == "a"
    assert column.fields[0].dtype == "integer"
    assert column.fields[1].name == "b"
    assert column.fields[1].dtype == "varchar"

def test_create_without_fields():
    column = DuckDBColumn.create(column="integer_test", dtype="integer")
    assert column.name == "integer_test"
    assert column.dtype == "integer"
    assert len(column.fields) == 0
