from dataclasses import dataclass
from typing import Any
from typing import Dict

from dbt.contracts.graph.nodes import SourceDefinition


@dataclass
class SourceConfig:
    name: str
    identifier: str
    schema: str
    database: str
    meta: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        base = {
            "name": self.name,
            "identifier": self.identifier,
            "schema": self.schema,
            "database": self.database,
        }
        base.update(self.meta)
        return base

    @classmethod
    def create(cls, source: SourceDefinition) -> "SourceConfig":
        meta = source.source_meta.copy()
        meta.update(source.meta)
        return SourceConfig(
            name=source.name,
            identifier=source.identifier,
            schema=source.schema,
            database=source.database,
            meta=meta,
        )
