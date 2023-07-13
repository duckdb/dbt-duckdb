from typing import Any
from typing import Dict

from duckdb import DuckDBPyConnection

from . import BasePlugin


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self._token = config.get("token")

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.load_extension("motherduck")
        connect_stmt = "PRAGMA md_connect"
        if self._token:
            connect_stmt = f"PRAGMA md_connect('token={self._token}')"
        conn.execute(connect_stmt)
