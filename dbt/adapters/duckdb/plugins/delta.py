from typing import Any
from typing import Dict
import duckdb

from deltalake import DeltaTable 

from . import BasePlugin
from ..utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        #place for init catalog in the future
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception("'delta_table_path' is a required argument for the delta table!")
        print(source_config)
        table_path = source_config["delta_table_path"]
        storage_options = source_config.get("storage",None)
        
        if storage_options: 
            dt = DeltaTable(table_path, storage_options)
        else:
            dt = DeltaTable(table_path)

        #delta attributes
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)

        if as_of_version:
            dt.load_version(1)
        
        if as_of_datetime:
            dt.load_with_datetime(as_of_datetime)

        #prunning attributes
        pruning_filter = source_config.get("pruning_filter", "1=1")
        pruning_projection = source_config.get("pruning_projection", "*")

        df_db = duckdb.arrow(dt.to_pyarrow_table())
        df_db_pruned = df_db.filter(pruning_filter).project(pruning_projection)

        print(df_db_pruned.explain())
        return df_db_pruned

#TODO each node calls plugin.load indipendent which is maybe overhead?

#Future
#TODO add deltalake storage options 
#TODO add databricks catalog 