import re
from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Tuple

from dbt.adapters.base.column import Column


@dataclass
class DuckDBColumn(Column):
    fields: List["DuckDBColumn"] = field(default_factory=list)

    def __post_init__(self):
        if self.is_struct():
            self._parse_struct_fields()

    def _parse_struct_fields(self):
        # In DuckDB, structs are defined as STRUCT(key1 type1, key2 type2, ...).
        # We need to extract the key-type pairs from the struct definition,
        # e.g., STRUCT(a VARCHAR, b INTEGER) -> ["a VARCHAR", "b INTEGER"].
        # Types can contain commas (DECIMAL(10, 2)) and nested structs, and keys
        # that need quoting come back double-quoted with embedded quotes doubled,
        # e.g. STRUCT("first name" VARCHAR, "a,b" INTEGER), so commas and spaces
        # only separate fields when they sit outside quotes and parentheses.
        match = re.match(r"STRUCT\((.*)\)", self.dtype, re.IGNORECASE)
        if not match:
            return

        for field_def in _split_struct_fields(match.group(1)):
            col_name, col_type = _split_field_name(field_def)
            self.fields.append(DuckDBColumn(column=col_name, dtype=col_type))

    def is_float(self):
        return self.dtype.lower() in {
            # floats
            "real",
            "float",
            "float4",
            "float8",
            "double",
        }

    def is_integer(self) -> bool:
        return self.dtype.lower() in {
            # signed types
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "hugeint",
            # unsigned types
            "utinyint",
            "usmallint",
            "uinteger",
            "ubigint",
            # aliases
            "int1",
            "int2",
            "int4",
            "int8",
            "short",
            "int",
            "signed",
            "long",
        }

    def is_struct(self) -> bool:
        return self.dtype.lower().startswith("struct")

    def flatten(self) -> List["DuckDBColumn"]:
        if not self.is_struct():
            return [self]

        flat_columns: List["DuckDBColumn"] = []
        for column_field in self.fields:
            if column_field.is_struct():
                # Recursively flatten nested structs
                for nested_field in column_field.flatten():
                    flat_columns.append(
                        DuckDBColumn(
                            column=f"{self.column}.{nested_field.column}",
                            dtype=nested_field.dtype,
                        )
                    )
            else:
                flat_columns.append(
                    DuckDBColumn(
                        column=f"{self.column}.{column_field.column}",
                        dtype=column_field.dtype,
                    )
                )
        return flat_columns


_QUOTED_FIELD_NAME = re.compile(r'"((?:[^"]|"")*)"\s*(.*)', re.DOTALL)


def _split_struct_fields(content: str) -> List[str]:
    """Split a struct body on the commas outside quotes and parentheses."""
    fields = []
    paren_level = 0
    in_quotes = False
    current_field = ""
    for char in content:
        if char == '"':
            in_quotes = not in_quotes
        elif not in_quotes:
            if char == "(":
                paren_level += 1
            elif char == ")":
                paren_level -= 1
            elif char == "," and paren_level == 0:
                fields.append(current_field.strip())
                current_field = ""
                continue
        current_field += char
    fields.append(current_field.strip())
    return [f for f in fields if f]


def _split_field_name(field_def: str) -> Tuple[str, str]:
    """Split a 'name TYPE' field definition, unquoting a quoted name."""
    quoted = _QUOTED_FIELD_NAME.match(field_def)
    if quoted:
        return quoted.group(1).replace('""', '"'), quoted.group(2)
    col_name, _, col_type = field_def.partition(" ")
    return col_name, col_type
