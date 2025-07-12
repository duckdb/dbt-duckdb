from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from dbt.adapters.base.column import Column


@dataclass
class DuckDBColumn(Column):
    # https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/adapters/bigquery/column.py
    fields: List[DuckDBColumn] = field(default_factory=list)

    def __post_init__(self):
        # Convert any dicts in self.fields to DuckDBColumn objects
        self.fields = [
            f if isinstance(f, DuckDBColumn) else DuckDBColumn.create_from_dict(f)
            for f in self.fields
        ]

    @classmethod
    def create_from_dict(cls, column_dict: Dict[str, Any]) -> DuckDBColumn:
        if isinstance(column_dict, DuckDBColumn):
            return column_dict

        return DuckDBColumn(
            column=column_dict["column"],
            dtype=column_dict["dtype"],
            char_size=column_dict.get("char_size"),
            numeric_precision=column_dict.get("numeric_precision"),
            numeric_scale=column_dict.get("numeric_scale"),
            fields=column_dict.get("fields", []),
        )

    @classmethod
    def create(
        cls, column: str, dtype: str, fields: Optional[List[DuckDBColumn]] = None
    ) -> DuckDBColumn:
        return DuckDBColumn(column=column, dtype=dtype, fields=fields or [])

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

    @property
    def name(self) -> str:
        return self.column

    def flatten(self) -> List[DuckDBColumn]:
        if not self.is_struct():
            return [self]

        flat_columns: List[DuckDBColumn] = []
        for column_field in self.fields:
            if column_field.is_struct():
                # Recursively flatten the nested struct
                nested_fields = column_field.flatten()
                for nested_field in nested_fields:
                    flat_columns.append(
                        DuckDBColumn.create(
                            f"{self.name}.{nested_field.name}",
                            nested_field.dtype,
                            fields=nested_field.fields,
                        )
                    )
            else:
                flat_columns.append(
                    DuckDBColumn.create(
                        f"{self.name}.{column_field.name}",
                        column_field.dtype,
                        fields=column_field.fields,
                    )
                )
        return flat_columns
