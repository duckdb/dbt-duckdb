from dataclasses import dataclass
from string import Template
from typing import Any
from typing import Optional
from typing import Type

from .connections import DuckDBConnectionManager
from .utils import SourceConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import HasQuoting
from dbt.adapters.contracts.relation import RelationConfig


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    require_alias: bool = False
    external: Optional[str] = None

    @classmethod
    def create_from(
        cls: Type["DuckDBRelation"],
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs: Any,
    ) -> "DuckDBRelation":
        if relation_config.resource_type == "source":
            return cls.create_from_source(quoting, relation_config, **kwargs)
        else:
            return super().create_from(quoting, relation_config, **kwargs)

    @classmethod
    def create_from_source(
        cls: Type["DuckDBRelation"], quoting: HasQuoting, source: RelationConfig, **kwargs: Any
    ) -> "DuckDBRelation":
        """
        This method creates a new DuckDBRelation instance from a source definition.
        It first checks if a 'plugin' is defined in the meta argument for the source or its parent configuration.
        If a 'plugin' is defined, it uses the environment associated with this run to get the name of the source that we should reference in the compiled model.
        If an 'external_location' is defined, it formats the location based on the 'formatter' defined in the source configuration.
        If the 'formatter' is not recognized, it raises a ValueError.
        Finally, it calls the parent class's create_from_source method to create the DuckDBRelation instance.

        :param cls: The class that this method is a part of.
        :param source: The source definition to create the DuckDBRelation from.
        :param kwargs: Additional keyword arguments.
        :return: A new DuckDBRelation instance.
        """
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

        return super().create_from(quoting, source, **kwargs)  # type: ignore

    def render(self) -> str:
        if self.external:
            return self.external
        else:
            return super().render()
