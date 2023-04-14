from typing import Any
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from . import Plugin
from ..utils import SourceConfig


class SQLAlchemyPlugin(Plugin):
    def __init__(self, plugin_config: Dict[str, Any]):
        self.engine = create_engine(plugin_config["connection_url"])

    def load(self, source_config: SourceConfig) -> pd.DataFrame:
        if "query" in source_config.meta:
            query = source_config.meta["query"]
            query = query.format(**source_config.as_dict())
            params = source_config.meta.get("params", {})
            with self.engine.connect() as conn:
                return pd.read_sql_query(text(query), con=conn, params=params)
        else:
            if "table" in source_config.meta:
                table = source_config.meta["table"]
            else:
                table = source_config.table_name()
            with self.engine.connect() as conn:
                return pd.read_sql_table(table, con=conn)
