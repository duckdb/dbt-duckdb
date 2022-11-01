from typing import Any, Optional, Type

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Self
from dbt.contracts.graph.parsed import ParsedSourceDefinition


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    external_location: Optional[str] = None

    @classmethod
    def create_from_source(cls: Type[Self], source: ParsedSourceDefinition, **kwargs: Any) -> Self:
        if source.external:
            # TODO(jwills): this is trivial and could be made much richer, just want to know
            # if it works
            kwargs["external_location"] = f"'{source.external.location}'"
        return super().create_from_source(source, **kwargs)

    def render(self) -> str:
        if self.external_location:
            return self.external_location
        else:
            return super().render()
