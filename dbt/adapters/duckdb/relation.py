from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.relation import Self
from dbt.contracts.graph.nodes import SourceDefinition

from .connections import DuckDBConnectionManager


def _meta_helper(field: str, source: SourceDefinition) -> Optional[Any]:
    if field in source.meta:
        return source.meta[field]
    elif field in source.source_meta:
        return source.source_meta[field]
    return None


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    external_location: Optional[str] = None

    @classmethod
    def create_from_source(cls: Type[Self], source: SourceDefinition, **kwargs: Any) -> Self:

        # Some special handling here to allow sources that are external files to be specified
        # via a `external_location` meta field. If the source's meta field is used, we include
        # some logic to allow basic templating of the external location based on the individual
        # name or identifier for the table itself to cut down on boilerplate.
        ext_location = _meta_helper("external_location", source)
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

        # Logic for handling sources defined in external catalogs
        ext_catalog = _meta_helper("external_catalog", source)
        if ext_catalog:
            catalog = DuckDBConnectionManager.CATALOGS.get(ext_catalog)
            if not catalog:
                raise Exception("Rarrrr external catalog not found: " + ext_catalog)
            pyarrow_obj = catalog.load(source.database, source.identifier)
            if not pyarrow_obj:
                raise Exception("Rarrrr pyarrow object not found in catalog")
            registered_name = f"{catalog.name}__{source.database}_{source.identifier}"
            DuckDBConnectionManager.register_pyarrow_as(pyarrow_obj, registered_name)
            kwargs["external_location"] = registered_name

        return super().create_from_source(source, **kwargs)  # type: ignore

    def render(self) -> str:
        if self.external_location:
            return self.external_location
        else:
            return super().render()
