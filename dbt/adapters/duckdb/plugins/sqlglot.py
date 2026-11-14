from typing import Any
from typing import Dict

import sqlglot

from . import BasePlugin
from ..environments.local import DuckDBCursorWrapper


class SqlglotWrapper(DuckDBCursorWrapper):
    def __init__(self, cursor, sql_from: str):
        self.sql_from = sql_from
        self._cursor = cursor

    def execute(self, sql, bindings=None):
        sql = sqlglot.transpile(sql, read=self.sql_from, write="duckdb").pop()
        if bindings is None:
            return self._cursor.execute(sql)
        else:
            return self._cursor.execute(sql, bindings)


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self.sql_from = plugin_config.get("sql_from", "duckdb")

    def modify_cursor(self, cursor):
        return SqlglotWrapper(cursor, self.sql_from)
