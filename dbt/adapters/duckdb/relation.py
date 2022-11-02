from typing import Any, Optional, Type

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Self
from dbt.clients.jinja import get_environment
from dbt.contracts.graph.parsed import ParsedSourceDefinition


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    external_location: Optional[str] = None

    @classmethod
    def create_from_source(cls: Type[Self], source: ParsedSourceDefinition, **kwargs: Any) -> Self:
        if "external_location" in source.meta or "external_location" in source.source_meta:
            env = get_environment()
            if "external_location" in source.meta:
                template = env.from_string(source.meta["external_location"])
            else:
                template = env.from_string(source.source_meta["external_location"])
            kwargs["external_location"] = template.render(source.to_dict())
        return super().create_from_source(source, **kwargs)  # type: ignore

    def render(self) -> str:
        if self.external_location:
            return self.external_location
        else:
            return super().render()
