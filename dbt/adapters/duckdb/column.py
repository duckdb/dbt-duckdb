from dataclasses import dataclass
from dataclasses import field
from typing import List

from dbt.adapters.base.column import Column


@dataclass
class DuckDBColumn(Column):
    fields: List["DuckDBColumn"] = field(default_factory=list)

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
        for field in self.fields:
            if field.is_struct():
                # Recursively flatten nested structs
                for nested_field in field.flatten():
                    flat_columns.append(
                        DuckDBColumn(
                            column=f"{self.column}.{nested_field.column}",
                            dtype=nested_field.dtype,
                        )
                    )
            else:
                flat_columns.append(
                    DuckDBColumn(
                        column=f"{self.column}.{field.column}",
                        dtype=field.dtype,
                    )
                )
        return flat_columns
