from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from duckdb.duckdb import DatabaseError
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_incrementing

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


def find_secrets_by_type(secrets: list[dict], secret_type: str) -> dict:
    """Find secrets of a specific type in the secrets dictionary."""
    for secret in secrets:
        if secret.get("type") == secret_type:
            return secret
    raise SecretTypeMissingError(f"Secret type {secret_type} not found in the secrets!")


class SecretTypeMissingError(Exception):
    """Exception raised when the secret type is missing from the secrets dictionary."""

    pass


def get_retry_decorator(max_attempts: int, wait_time: float, exception: DatabaseError):
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_incrementing(start=wait_time, increment=0.05),
        retry=retry_if_exception_type(exception),
        reraise=True,
    )
