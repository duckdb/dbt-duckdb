from typing import Any
from typing import Dict

import sqlglot
from dbt_common.exceptions import DbtRuntimeError

from . import BasePlugin


class TranspiledCursor:
    def __init__(self, cursor, trans_from: str):
        self.trans_from = trans_from
        self._cursor = cursor

    # forward along all non-execute() methods/attribute look-ups
    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, sql, bindings=None):
        sql = sqlglot.transpile(sql, read=self.trans_from, write="duckdb").pop()
        try:
            if bindings is None:
                return self._cursor.execute(sql)
            else:
                return self._cursor.execute(sql, bindings)
        except RuntimeError as e:
            raise DbtRuntimeError(str(e))


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self.trans_from = plugin_config.get("trans_from", "duckdb")

    def cursor(self, cursor):
        return TranspiledCursor(cursor, self.trans_from)
