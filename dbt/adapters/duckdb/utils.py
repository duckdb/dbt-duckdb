from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from dbt.adapters.base.column import Column
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationConfig
# TODO
# from dbt.context.providers import RuntimeConfigObject


@dataclass
class SourceConfig:
    name: str
    identifier: str
    schema: str
    database: Optional[str]
    meta: Dict[str, Any]
    tags: List[str]

    def get(self, key, default=None):
        return self.meta.get(key, default)

    def __getitem__(self, key):
        return self.meta[key]

    def __contains__(self, key):
        return key in self.meta

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
            "tags": self.tags,
        }
        base.update(self.meta)
        return base

    @classmethod
    def create_from_source(cls, source: RelationConfig) -> "SourceConfig":
        meta = source.meta.copy()
        # Use the config properties as well if they are present
        config_properties = source.config.extra if source.config else {}
        meta.update(config_properties)
        return SourceConfig(
            name=source.name,
            identifier=source.identifier,
            schema=source.schema,
            database=source.database,
            meta=meta,
            tags=source.tags or [],
        )


@dataclass
class TargetLocation:
    path: str
    format: str

    def as_dict(self) -> Dict[str, Any]:
        return {"path": self.path, "format": self.format}


@dataclass
class TargetConfig:
    relation: BaseRelation
    column_list: Sequence[Column]
    config: Any  # TODO
    location: Optional[TargetLocation] = None

    def as_dict(self) -> Dict[str, Any]:
        base = {
            "relation": self.relation.to_dict(),
            "column_list": [{"column": c.column, "dtype": c.dtype} for c in self.column_list],
            "config": self.config,
        }
        if self.location:
            base["location"] = self.location.as_dict()
        return base
