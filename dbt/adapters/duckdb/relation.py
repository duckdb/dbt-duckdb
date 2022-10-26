import os
from typing import Optional, TypeVar, Type

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.dataclass_schema import StrEnum

Self = TypeVar("Self", bound="DuckDBRelation")

class DuckDBRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    Parquet = "parquet"
    CSV = "csv"

@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    type: Optional[DuckDBRelationType] = None

    def render(self) -> str:
        if self.type in ["csv", "parquet"]:
            db_dir = os.path.dirname(self.database)
            db_dir += ("/" if db_dir != "" else "")
            if self.type == "csv":
                # TODO: use read_csv_auto(& infer params from config)... see https://duckdb.org/docs/data/csv
                return f"{db_dir+self.database}.csv"
            # read_parquet isn't truly required, but I'm making this explicit so that config can be passed if desired.
            if self.type == "parquet":
                return f"read_parquet({db_dir+self.database}.parquet)"
        else:
            return super().render()
