from typing import Any
from typing import Dict

from duckdb import DuckDBPyConnection

from . import BasePlugin
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.version import __version__


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self._token = config.get("token")

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.load_extension("motherduck")

    def update_connection_config(self, creds: DuckDBCredentials, config: Dict[str, Any]):
        user_agent = f"dbt/{__version__}"
        if "custom_user_agent" in config:
            user_agent = f"{user_agent} {config['custom_user_agent']}"

        config["custom_user_agent"] = user_agent

        # If a user specified the token via the plugin config,
        # pass it to the config kwarg in duckdb.connect
        if creds.token_from_config != "":
            config["motherduck_token"] = creds.token_from_config
