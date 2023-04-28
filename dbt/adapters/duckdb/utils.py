from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional

from dbt.contracts.graph.nodes import SourceDefinition


@dataclass
class SourceConfig:
    name: str
    identifier: str
    schema: str
    database: Optional[str]
    meta: Dict[str, Any]

    def table_name(self) -> str:
        if self.database:
            return ".".join([self.database, self.schema, self.identifier])
        else:
            return ".".join([self.schema, self.identifier])

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
        # Use the config properties as well if they are present
        meta.update(source.config._extra)
        return SourceConfig(
            name=source.name,
            identifier=source.identifier,
            schema=source.schema,
            database=source.database,
            meta=meta,
        )
