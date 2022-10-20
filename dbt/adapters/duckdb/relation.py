import os
from typing import Optional, Type

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty


class DuckDBRelationType(StrEnum):
    # Built-in materialization types.
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"

    # DuckDB-specific materialization types.
    CSV = "csv"
    Parquet = "parquet"


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    type: Optional[DuckDBRelationType] = None

    @classproperty
    def get_relation_type(cls) -> Type[DuckDBRelationType]:
        return DuckDBRelationType

    def is_file(self) -> bool:
        return self.type in {DuckDBRelationType.CSV, DuckDBRelationType.Parquet}

    def is_parquet(self) -> bool:
        return self.type == DuckDBRelationType.Parquet

    def is_csv(self) -> bool:
        return self.type == DuckDBRelationType.CSV

    def render(self):
        if self.is_file() or (
            self.database is not None and os.path.sep in self.database
        ):
            identifier = self.identifier
            if self.is_file():
                identifier += f".{self.type.value}"
            path_str = os.path.join(self.database, self.schema, identifier)
            return f"'{path_str}'"
        else:
            return super().render()
