from typing import Any
from typing import Dict
from urllib.parse import parse_qs
from urllib.parse import urlparse

from duckdb import DuckDBPyConnection

from . import BasePlugin
from dbt.adapters.duckdb.__version__ import version as __plugin_version__
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.version import __version__

TOKEN = "token"
MOTHERDUCK_TOKEN = "motherduck_token"
CUSTOM_USER_AGENT = "custom_user_agent"
MOTHERDUCK_EXT = "motherduck"
MOTHERDUCK_CONFIG_OPTIONS = [MOTHERDUCK_TOKEN]


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self._token = config.get(TOKEN) or config.get(TOKEN.lower())

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.load_extension(MOTHERDUCK_EXT)
        # If a MotherDuck database is in attachments,
        # set config options *before* attaching
        if self.creds is not None and self.creds.is_motherduck_attach:
            # Check if the config options are specified in the path
            for attachment in self.creds.motherduck_attach:
                parsed = urlparse(attachment.path)
                qs = parse_qs(parsed.query)
                for KEY in MOTHERDUCK_CONFIG_OPTIONS:
                    value = qs.get(KEY)
                    if value:
                        conn.execute(f"SET {KEY} = '{value[0]}'")
            # If config options are specified via plugin config, set them here
            if self._token:
                conn.execute(f"SET {MOTHERDUCK_TOKEN} = '{self._token}'")
            elif self.creds.settings:
                if MOTHERDUCK_TOKEN in self.creds.settings:
                    token = self.creds.settings.pop(MOTHERDUCK_TOKEN)
                    conn.execute(f"SET {MOTHERDUCK_TOKEN} = '{token}'")

    @staticmethod
    def token_from_config(creds: DuckDBCredentials) -> str:
        """Load the token from the MotherDuck plugin config
        If not specified, this returns an empty string

        :param str: MotherDuck token
        """
        plugins = creds.plugins or []
        for plugin in plugins:
            if plugin.config:
                if (
                    TOKEN in plugin.config
                    or TOKEN.upper() in plugin.config
                    or MOTHERDUCK_TOKEN in plugin.config
                    or MOTHERDUCK_TOKEN.upper() in plugin.config
                ):
                    token = (
                        plugin.config.get(TOKEN)
                        or plugin.config.get(TOKEN.upper())
                        or plugin.config.get(MOTHERDUCK_TOKEN)
                        or plugin.config.get(MOTHERDUCK_TOKEN.upper())
                    )
                    return str(token)
        return ""

    def update_connection_config(self, creds: DuckDBCredentials, config: Dict[str, Any]):
        user_agent = f"dbt/{__version__} dbt-duckdb/{__plugin_version__}"
        if CUSTOM_USER_AGENT in config:
            user_agent = f"{user_agent} {config[CUSTOM_USER_AGENT]}"
        settings: Dict[str, Any] = creds.settings or {}
        if CUSTOM_USER_AGENT in settings:
            user_agent = f"{user_agent} {settings.pop(CUSTOM_USER_AGENT)}"

        config[CUSTOM_USER_AGENT] = user_agent

        # If a user specified MotherDuck config options via the plugin config,
        # pass it to the config kwarg in duckdb.connect.
        if not creds.is_motherduck_attach:
            token = self.token_from_config(creds)
            if token != "":
                config[MOTHERDUCK_TOKEN] = token
