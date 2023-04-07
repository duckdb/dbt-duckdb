from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import Type

from .connections import DuckDBConnectionManager
from .utils import SourceConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.relation import Self
from dbt.contracts.graph.nodes import SourceDefinition


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    external: Optional[str] = None

    @classmethod
    def create_from_source(cls: Type[Self], source: SourceDefinition, **kwargs: Any) -> Self:
        source_config = SourceConfig.create(source)
        # First check to see if a 'plugin' is defined in the meta argument for
        # the source or its parent configuration, and if it is, use the environment
        # associated with this run to get the name of the source that we should
        # reference in the compiled model
        if "plugin" in source_config.meta:
            plugin_name = source_config.meta["plugin"]
            DuckDBConnectionManager.env().load_source(plugin_name, source_config)
        elif "external_location" in source_config.meta:
            # Call str.format with the schema, name and identifier for the source so that they
            # can be injected into the string; this helps reduce boilerplate when all
            # of the tables in the source have a similar location based on their name
            # and/or identifier.
            ext_location = source_config.meta["external_location"].format(
                **source_config.as_dict()
            )
            # If it's a function call or already has single quotes, don't add them
            if "(" not in ext_location and not ext_location.startswith("'"):
                ext_location = f"'{ext_location}'"
            kwargs["external"] = ext_location

        return super().create_from_source(source, **kwargs)  # type: ignore

    def render(self) -> str:
        if self.external:
            return self.external
        else:
            return super().render()
