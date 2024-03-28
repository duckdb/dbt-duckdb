import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig


def foo() -> int:
    return 1729


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function("foo", foo)

    def store(self, df: DuckDBPyRelation, target_config: TargetConfig, cursor=None):
        assert target_config.config.get("key") == "value"

    def can_be_upstream_referenced(self):
        return True

    def load(self, source_config: SourceConfig, cursor=None):
        return duckdb.sql("SELECT 1729 as foo").arrow()

    def create_source_config(self, target_config: TargetConfig) -> SourceConfig:
        source_config = SourceConfig(
            name=target_config.relation.name,
            identifier=target_config.relation.identifier,
            schema=target_config.relation.schema,
            database=target_config.relation.database,
            meta=target_config.as_dict(),
            tags=[],
        )
        return source_config
