from typing import Any
from typing import Dict

from duckdb import DuckDBPyConnection

from . import BasePlugin


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self._dsn = config.get("dsn")
        if self._dsn is None:
            raise Exception("'dsn' is a required argument for the postgres plugin!")
        self._source_schema = config.get("source_schema", "public")
        self._sink_schema = config.get("sink_schema", "main")
        self._overwrite = config.get("overwrite", False)
        self._filter_pushdown = config.get("filter_pushdown", False)

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.install_extension("postgres")
        conn.load_extension("postgres")

        if self._sink_schema:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._sink_schema}")

        attach_args = [
            ("source_schema", f"'{self._source_schema}'"),
            ("sink_schema", f"'{self._sink_schema}'"),
            ("overwrite", str(self._overwrite).lower()),
            ("filter_pushdown", str(self._filter_pushdown).lower()),
        ]
        attach_stmt = (
            f"CALL postgres_attach('{self._dsn}', {', '.join(f'{k}={v}' for k, v in attach_args)})"
        )
        conn.execute(attach_stmt)
