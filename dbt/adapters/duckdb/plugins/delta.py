from typing import Any
from typing import Dict
import duckdb

from deltalake import DeltaTable

from . import BasePlugin
from ..utils import SourceConfig
from dbt.logger import GLOBAL_LOGGER as logger


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        # place for init catalog in the future
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception(
                "'delta_table_path' is a required argument for the delta table!"
            )
        #logger.debug(source_config)
        table_path = source_config["delta_table_path"]
        storage_options = source_config.get("storage", None)

        if storage_options:
            dt = DeltaTable(table_path, storage_options)
        else:
            dt = DeltaTable(table_path)

        # delta attributes
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)

        if as_of_version:
            dt.load_version(as_of_version)

        if as_of_datetime:
            dt.load_with_datetime(as_of_datetime)

        return dt.to_pyarrow_table()

# Future
# TODO add deltalake storage options
# TODO add databricks catalog
