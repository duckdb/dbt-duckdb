import threading
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import List
from typing import Optional

import duckdb

from dbt.adapters.base.column import Column


@dataclass
class DuckDBColumn(Column):
    fields: List["DuckDBColumn"] = field(default_factory=list)

    def __post_init__(self):
        if self.is_struct():
            self._parse_struct_fields()

    def _parse_struct_fields(self):
        # Let DuckDB parse the type, so quoted keys (spaces, commas, reserved
        # words, doubled quotes), nested structs and parameterized types such as
        # DECIMAL(10, 2) come back as (name, type) pairs. A type DuckDB cannot
        # resolve on its own, such as a user-defined ENUM, is left unflattened,
        # and so is a list of structs, STRUCT(...)[], which is not a struct.
        struct_type = _parse_type(self.dtype)
        if struct_type is None or struct_type.id != "struct":
            return
        for name, child_type in struct_type.children:
            self.fields.append(DuckDBColumn(column=name, dtype=str(child_type)))

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
        if not self.fields:
            return [self]

        flat_columns: List["DuckDBColumn"] = []
        for column_field in self.fields:
            # a nested struct flattens recursively; any other field returns itself
            for nested_field in column_field.flatten():
                flat_columns.append(
                    DuckDBColumn(
                        column=f"{self.column}.{nested_field.column}",
                        dtype=nested_field.dtype,
                    )
                )
        return flat_columns


_type_parser: Optional[duckdb.DuckDBPyConnection] = None
_type_parser_lock = threading.Lock()


def _parse_type(dtype: str) -> Optional[Any]:
    """Parse a type string with a private in-memory connection, or return None."""
    global _type_parser
    with _type_parser_lock:
        if _type_parser is None:
            _type_parser = duckdb.connect(":memory:")
        try:
            return _type_parser.sqltype(dtype)
        except duckdb.Error:
            return None
