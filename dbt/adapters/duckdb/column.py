import re
from dataclasses import dataclass
from dataclasses import field
from typing import List

from dbt.adapters.base.column import Column


@dataclass
class DuckDBColumn(Column):
    fields: List["DuckDBColumn"] = field(default_factory=list)

    def __post_init__(self):
        if self.is_struct():
            self._parse_struct_fields()

    def _parse_struct_fields(self):
        # In DuckDB, structs are defined as STRUCT(key1 type1, key2 type2, ...)
        # We need to extract the key-type pairs from the struct definition
        # e.g., STRUCT(a VARCHAR, b INTEGER) -> ["a VARCHAR", "b INTEGER"]
        # We can't just split by comma, because types can contain commas
        # e.g. DECIMAL(10, 2)
        # The following logic will handle nested structs and complex types
        match = re.match(r"STRUCT\((.*)\)", self.dtype, re.IGNORECASE)
        if not match:
            return

        content = match.group(1)

        fields = []
        paren_level = 0
        current_field = ""
        for char in content:
            if char == "(":
                paren_level += 1
            elif char == ")":
                paren_level -= 1

            if char == "," and paren_level == 0:
                fields.append(current_field.strip())
                current_field = ""
            else:
                current_field += char
        fields.append(current_field.strip())

        for f in fields:
            # Split on the first space to separate the name from the type
            parts = f.split(" ", 1)
            col_name = parts[0]
            col_type = parts[1]
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
