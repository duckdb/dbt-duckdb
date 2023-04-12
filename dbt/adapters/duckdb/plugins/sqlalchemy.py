from typing import Any
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine

from . import Plugin
from ..utils import SourceConfig


class SQLAlchemyPlugin(Plugin):
    def __init__(self, plugin_config: Dict[str, Any]):
        self.engine = create_engine(plugin_config["connection_url"])

    def load(self, source_config: SourceConfig) -> pd.DataFrame:
        query = source_config.meta["query"]
        query = query.format(**source_config.as_dict())
        params = source_config.meta.get("params", {})
        return pd.read_sql_query(query, con=self.engine, params=params)
