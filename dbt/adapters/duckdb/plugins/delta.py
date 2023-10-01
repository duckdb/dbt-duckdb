from typing import Any
from typing import Dict
import duckdb

from deltalake import DeltaTable 

from . import BasePlugin
from ..utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception("'delta_table_path' is a required argument for the delta table!")
        
        table_path = source_config["delta_table_path"]

        pruning_filter = source_config.get("pruning_filter", "1=1")
        pruning_projection = source_config.get("pruning_projection", "*")

        ##TODO check if table is there and path is ok

        dt = DeltaTable(table_path)
        df_db = duckdb.arrow(dt.to_pyarrow_table())
        df_db_pruned = df_db.filter(pruning_filter).project(pruning_projection)

        print(df_db_pruned.explain())
        return df_db_pruned

#TODO each node calls plugin.load indipendent which is maybe overhead?

#Future
#TODO add time travel; add deltalake storage options 
#TODO add databricks catalog 