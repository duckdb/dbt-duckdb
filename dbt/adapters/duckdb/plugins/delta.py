from typing import Any
from typing import Dict

from deltalake import DeltaTable

from . import BasePlugin
from ..utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        pass

    def configure_cursor(self, cursor):
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception("'delta_table_path' is a required argument for the delta table!")

        table_path = source_config["delta_table_path"]
        storage_options = source_config.get("storage_options", None)

        if storage_options:
            dt = DeltaTable(table_path, storage_options=storage_options)
        else:
            dt = DeltaTable(table_path)

        # delta attributes
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)

        if as_of_version:
            dt.load_as_version(as_of_version)

        if as_of_datetime:
            dt.load_as_version(as_of_datetime)

        df = dt.to_pyarrow_dataset()

        return df

    def default_materialization(self):
        return "view"


# Future
# TODO add databricks catalog
