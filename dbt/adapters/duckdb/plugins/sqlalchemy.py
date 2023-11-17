from typing import Any
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from . import BasePlugin
from . import pd_utils
from ..utils import SourceConfig
from ..utils import TargetConfig


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self.engine = create_engine(plugin_config.pop("connection_url"), **plugin_config)

    def load(self, source_config: SourceConfig) -> pd.DataFrame:
        if "query" in source_config:
            query = source_config["query"]
            query = query.format(**source_config.as_dict())
            params = source_config.get("params", {})
            with self.engine.connect() as conn:
                return pd.read_sql_query(text(query), con=conn, params=params)
        else:
            if "table" in source_config:
                table = source_config["table"]
            else:
                table = source_config.table_name()
            with self.engine.connect() as conn:
                return pd.read_sql_table(table, con=conn)

    def store(self, target_config: TargetConfig):
        # first, load the data frame from the external location
        df = pd_utils.target_to_df(target_config)
        table_name = target_config.relation.identifier
        # then, write it to the database
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def __del__(self):
        self.engine.dispose()
        self.engine = None
