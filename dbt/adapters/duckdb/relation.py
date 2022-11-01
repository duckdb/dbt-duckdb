import os
from typing import Optional, TypeVar, Type, Any

from dataclasses import dataclass

import dbt

from dbt.adapters.base.relation import BaseRelation
from dbt.dataclass_schema import StrEnum

from dbt.exceptions import InternalException
from dbt.node_types import NodeType
from dbt.contracts.graph.compiled import CompiledNode
from dbt.contracts.graph.parsed import ParsedSourceDefinition, ParsedNode

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
            # db_dir = os.path.dirname(self.database)
            # db_dir += ("/" if db_dir != "" else "")
            if self.type == "csv":
                # TODO: use read_csv_auto(& infer params from config)... see https://duckdb.org/docs/data/csv
                return f"'{self.identifier}.csv'"
            # read_parquet isn't truly required, but I'm making this explicit so that config can be passed if desired.
            if self.type == "parquet":
                return f"read_parquet('{self.identifier}.parquet')"
        else:
            return super().render()
