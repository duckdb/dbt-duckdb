import os
from typing import Optional, Type, Iterator, Tuple

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import ComponentName
from dbt.adapters.duckdb import util
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
        if self.is_file() or util.is_filepath(self.database):
            pieces = [
                (ComponentName.Database, self.database),
                (ComponentName.Schema, self.schema),
                (ComponentName.Identifier, self.identifier),
            ]
            filtered = []
            for key, value in pieces:
                if self.include_policy.get_part(key):
                    filtered.append(value)
            path_str = os.path.sep.join(filtered)
            if not self.is_file():
                # ugh, horrible hack up in here
                if os.path.exists(path_str + ".parquet"):
                    path_str += ".parquet"
                elif os.path.exists(path_str + ".csv"):
                    path_str += ".csv"
            else:
                path_str += ".parquet" if self.is_parquet() else ".csv"
            return f"'{path_str}'"
        else:
            return super().render()
