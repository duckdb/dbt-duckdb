from duckdb import DuckDBPyConnection

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig


def foo() -> int:
    return 1729


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function("foo", foo)

    def store(self, target_config: TargetConfig, df=None):
        assert target_config.config.get("key") == "value"
