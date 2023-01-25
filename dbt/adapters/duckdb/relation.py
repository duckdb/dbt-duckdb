from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.relation import Self
from dbt.contracts.graph.nodes import SourceDefinition


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    external_location: Optional[str] = None

    @classmethod
    def create_from_source(cls: Type[Self], source: SourceDefinition, **kwargs: Any) -> Self:

        # Some special handling here to allow sources that are external files to be specified
        # via a `external_location` meta field. If the source's meta field is used, we include
        # some logic to allow basic templating of the external location based on the individual
        # name or identifier for the table itself to cut down on boilerplate.
        ext_location = None
        if "external_location" in source.meta:
            ext_location = source.meta["external_location"]
        elif "external_location" in source.source_meta:
            # Use str.format here to allow for some basic templating outside of Jinja
            ext_location = source.source_meta["external_location"]

        if ext_location:
            # Call str.format with the schema, name and identifier for the source so that they
            # can be injected into the string; this helps reduce boilerplate when all
            # of the tables in the source have a similar location based on their name
            # and/or identifier.
            ext_location = ext_location.format(
                schema=source.schema, name=source.name, identifier=source.identifier
            )
            # If it's a function call or already has single quotes, don't add them
            if "(" not in ext_location and not ext_location.startswith("'"):
                ext_location = f"'{ext_location}'"
            kwargs["external_location"] = ext_location

        return super().create_from_source(source, **kwargs)  # type: ignore

    def render(self) -> str:
        if self.external_location:
            return self.external_location
        else:
            return super().render()
