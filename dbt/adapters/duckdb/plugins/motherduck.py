from typing import Any
from typing import Dict
from urllib.parse import parse_qs
from urllib.parse import urlparse

from duckdb import DuckDBPyConnection

from . import BasePlugin
from dbt.adapters.duckdb.__version__ import version as __plugin_version__
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.version import __version__

CUSTOM_USER_AGENT = "custom_user_agent"
MOTHERDUCK_EXT = "motherduck"
# MotherDuck config options, in order in which they need to be set
# (SaaS mode is last because it locks other config options)
MOTHERDUCK_CONFIG_OPTIONS = [
    "motherduck_token",
    "motherduck_attach_mode",
    "motherduck_saas_mode",
]


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config

    @staticmethod
    def get_config_from_path(path):
        return {key: value[0] for key, value in parse_qs(urlparse(path).query).items()}

    @staticmethod
    def get_md_config_settings(config):
        # Get MotherDuck config settings
        md_config = {}
        for name in MOTHERDUCK_CONFIG_OPTIONS:
            for key in [
                name,
                name.replace("motherduck_", ""),
                name.upper(),
                name.replace("motherduck_", "").upper(),
            ]:
                if key in config:
                    md_config[name] = config[key]

        # Sort values (SaaS mode should be set last)
        return dict(
            sorted(
                md_config.items(),
                key=lambda x: MOTHERDUCK_CONFIG_OPTIONS.index(x[0]),
            )
        )

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.load_extension(MOTHERDUCK_EXT)
        # If a MotherDuck database is in attachments,
        # set config options *before* attaching
        if self.creds is not None and self.creds.is_motherduck_attach:
            config = {}

            # add config options specified in the path
            for attachment in self.creds.motherduck_attach:
                config.update(self.get_config_from_path(attachment.path))

            # add config options specified via plugin config
            config.update(self._config)

            # add config options specified via settings
            if self.creds.settings is not None:
                config.update(self.creds.settings)

            # set MD config options and remove from settings
            for key, value in self.get_md_config_settings(config).items():
                conn.execute(f"SET {key} = '{value}'")
                if self.creds.settings is not None and key in self.creds.settings:
                    self.creds.settings.pop(key)

    def update_connection_config(self, creds: DuckDBCredentials, config: Dict[str, Any]):
        user_agent = f"dbt/{__version__} dbt-duckdb/{__plugin_version__}"
        settings: Dict[str, Any] = creds.settings or {}
        custom_user_agent = config.get(CUSTOM_USER_AGENT) or settings.pop(CUSTOM_USER_AGENT, None)
        if custom_user_agent:
            user_agent = f"{user_agent} {custom_user_agent}"
        config[CUSTOM_USER_AGENT] = user_agent

        # If a user specified MotherDuck config options via the plugin config,
        # pass it to the config kwarg in duckdb.connect.
        if not creds.is_motherduck_attach:
            config.update(self.get_md_config_settings(self._config))
