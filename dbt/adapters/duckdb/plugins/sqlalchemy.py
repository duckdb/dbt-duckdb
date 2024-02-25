from typing import Any, Dict

import pandas as pd
from duckdb import DuckDBPyRelation
from sqlalchemy import create_engine, text

from ..utils import SourceConfig, TargetConfig
from . import BasePlugin

# overall i would recommend to use attach in duckdb but this can be nice for some cases where 
# native one is not supported e.g starrock 

# here we have to requestion the names of the tables 
class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self.engine = create_engine(plugin_config.pop("connection_url"), **plugin_config)

    def load(self, source_config: SourceConfig, coursor = None):
        if "query" in source_config:
            query = source_config["query"]
            query = query.format(**source_config.as_dict())
            params = source_config.get("params", {})
            with self.engine.connect() as conn:
                return pd.read_sql_query(text(query), con=conn, params=params)
        else:
            # we should question this? what is the use case?
            if "table" in source_config:
                table = source_config["table"]
            else:
                table = source_config.table_name()
            with self.engine.connect() as conn:
                return pd.read_sql_table(table, con=conn)

    def store(self, df: DuckDBPyRelation, target_config: TargetConfig, cursor = None):
        # first, load the data frame from the external location
        pd_df = df.df()
        table_name = target_config.relation.identifier
        # then, write it to the database
        pd_df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def can_be_upstream_referenced(self):
        return True
    
    def create_source_config(self, target_config: TargetConfig) -> SourceConfig:
        meta = {
            "table": target_config.relation.identifier
        }
        source_config = SourceConfig(
            name= target_config.relation.name,
            identifier= target_config.relation.identifier,
            schema=target_config.relation.schema,
            database=target_config.relation.database,
            meta= meta,
            tags= [],
        )
        return source_config

    def __del__(self):
        self.engine.dispose()
        self.engine = None
