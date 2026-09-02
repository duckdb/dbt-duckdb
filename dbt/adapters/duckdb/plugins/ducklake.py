from typing import Any
from typing import Dict

from duckdb import DuckDBPyConnection

from . import BasePlugin
from ..utils import escape_sql_string
from ..utils import TargetConfig

# ducklake_add_data_files options that may be set in the model or plugin config
PASSTHROUGH_OPTIONS = ["ignore_extra_columns", "allow_missing_columns"]


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class Plugin(BasePlugin):
    """Registers the parquet file written by an `external` materialization into an
    attached DuckLake catalog via ducklake_add_data_files, instead of copying the data.

    The target DuckLake database is set by the `database` plugin config or the
    `ducklake_database` model config; the table defaults to the model name and is
    created from the parquet schema if it does not exist yet.
    """

    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config

    def configure_connection(self, conn: DuckDBPyConnection):
        self._conn = conn

    def store(self, target_config: TargetConfig):
        assert target_config.location is not None
        assert target_config.relation.identifier is not None
        database = target_config.config.get("ducklake_database") or self._config.get("database")
        if not database:
            raise ValueError(
                "The ducklake plugin requires the DuckLake database alias, set via the "
                "'database' plugin config or the 'ducklake_database' model config"
            )
        schema = target_config.config.get("ducklake_schema") or self._config.get("schema", "main")
        table = target_config.config.get("ducklake_table") or target_config.relation.identifier
        path = target_config.location.path

        options = ""
        for option in PASSTHROUGH_OPTIONS:
            value = target_config.config.get(option, self._config.get(option))
            if value is not None:
                options += f", {option} => {'true' if value else 'false'}"

        qualified = f"{_ident(database)}.{_ident(schema)}.{_ident(table)}"
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                f"SELECT * FROM read_parquet('{escape_sql_string(path)}') LIMIT 0"
            )
            cursor.execute(
                f"CALL ducklake_add_data_files('{escape_sql_string(database)}', "
                f"'{escape_sql_string(table)}', '{escape_sql_string(path)}', "
                f"schema => '{escape_sql_string(schema)}'{options})"
            )
        finally:
            cursor.close()
