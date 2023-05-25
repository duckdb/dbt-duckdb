from duckdb import DuckDBPyConnection

from dbt.adapters.duckdb.plugins import BasePlugin


def foo() -> int:
    return 1729


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function("foo", foo)
