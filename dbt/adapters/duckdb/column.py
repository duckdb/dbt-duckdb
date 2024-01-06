from dataclasses import dataclass

from dbt.adapters.base.column import Column


@dataclass
class DuckDBColumn(Column):
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
