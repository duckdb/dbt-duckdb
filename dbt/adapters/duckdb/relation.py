from dataclasses import dataclass
from string import Template
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
        source_config = SourceConfig.create_from_source(source)
        # First check to see if a 'plugin' is defined in the meta argument for
        # the source or its parent configuration, and if it is, use the environment
        # associated with this run to get the name of the source that we should
        # reference in the compiled model
        if "plugin" in source_config:
            plugin_name = source_config["plugin"]
            if DuckDBConnectionManager._ENV is not None:
                # No connection means we are probably in the dbt parsing phase, so don't load yet.
                DuckDBConnectionManager.env().load_source(plugin_name, source_config)
        elif "external_location" in source_config:
            ext_location_template = source_config["external_location"]
            formatter = source_config.get("formatter", "newstyle")
            if formatter == "newstyle":
                ext_location = ext_location_template.format_map(source_config.as_dict())
            elif formatter == "oldstyle":
                ext_location = ext_location_template % source_config.as_dict()
            elif formatter == "template":
                ext_location = Template(ext_location_template).substitute(source_config.as_dict())
            else:
                raise ValueError(
                    f"Formatter {formatter} not recognized. Must be one of 'newstyle', 'oldstyle', or 'template'."
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
