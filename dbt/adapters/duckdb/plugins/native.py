import os
from typing import Any, Dict

from duckdb import DuckDBPyRelation

from ..utils import SourceConfig, TargetConfig
from . import BasePlugin

# here will be parquet,csv,json implementation,
# this plugin should be default one if none is specified
# we can change the name of the plugin


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        pass

    def configure_cursor(self, cursor):
        pass

    def default_materialization(self):
        return "view"

    # this one can be better not to go over some other format and df but directly
    # https://stackoverflow.com/questions/78055585/how-to-reference-duckdbpyrelation-from-another-connection
    def load(self, source_config: SourceConfig, coursor=None):
        location = external_read_location(
            source_config.meta.get("location", "").get("path"),
            source_config.meta.get("config", {}).get("options", {}),
        )
        return f"(SELECT * FROM '{location}')"

    def can_be_upstream_referenced(self):
        return True

    def create_source_config(self, target_config: TargetConfig) -> SourceConfig:
        source_config = SourceConfig(
            name=target_config.relation.name,
            identifier=target_config.relation.identifier,
            schema=target_config.relation.schema,
            database=target_config.relation.database,
            meta=target_config.as_dict(),
            tags=[],
        )
        return source_config

    def store(self, df: DuckDBPyRelation, target_config: TargetConfig, cursor=None):
        location = target_config.location.path
        options = external_write_options(
            location, target_config.config.get("options", {})
        )
        cursor.sql(f"COPY (SELECT * FROM df) to '{location}' ({options})")

    def adapt_target_config(self, target_config: TargetConfig) -> TargetConfig:
        # setup the location with default to parquet if not partitions_by
        if target_config.location.format == "default":
            target_config.location.format = "parquet"

        if "partition_by" not in target_config.config.get("options", {}):
            target_config.location.path = (
                target_config.location.path + "." + target_config.location.format
            )
        return target_config


# 1 to 1 from adapter
# TODO those can be maybe better written
def external_write_options(write_location: str, rendered_options: dict) -> str:
    if "format" not in rendered_options:
        ext = os.path.splitext(write_location)[1].lower()
        if ext:
            rendered_options["format"] = ext[1:]
        elif "delimiter" in rendered_options:
            rendered_options["format"] = "csv"
        else:
            rendered_options["format"] = "parquet"

    if rendered_options["format"] == "csv":
        if "header" not in rendered_options:
            rendered_options["header"] = 1

    if "partition_by" in rendered_options:
        v = rendered_options["partition_by"]
        if "," in v and not v.startswith("("):
            rendered_options["partition_by"] = f"({v})"

    ret = []
    for k, v in rendered_options.items():
        if k.lower() in {
            "delimiter",
            "quote",
            "escape",
            "null",
        } and not v.startswith("'"):
            ret.append(f"{k} '{v}'")
        else:
            ret.append(f"{k} {v}")
    return ", ".join(ret)


# 1 to 1 from adapter
def external_read_location(write_location: str, rendered_options: dict) -> str:
    if rendered_options.get("partition_by"):
        globs = [write_location, "*"]
        partition_by = str(rendered_options.get("partition_by"))
        globs.extend(["*"] * len(partition_by.split(",")))
        return ".".join(
            ["/".join(globs), str(rendered_options.get("format", "parquet"))]
        )
    return write_location
