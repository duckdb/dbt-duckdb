from typing import Any
from typing import Dict
from urllib.parse import parse_qs
from urllib.parse import urlparse

from duckdb import DuckDBPyConnection

from . import BasePlugin
from dbt.adapters.duckdb.__version__ import version as __plugin_version__
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.version import __version__

# MD config options, in the order in which they should be set
# Should be prefixed as motherduck_<option>
MOTHERDUCK_CONFIG_OPTIONS = [
    "token",
    "attach_mode",
    "saas_mode",
]
CUSTOM_USER_AGENT = "custom_user_agent"
MOTHERDUCK_EXT = "motherduck"


def _get_index(kv):
    key = kv[0].replace("motherduck_", "")
    if key in MOTHERDUCK_CONFIG_OPTIONS:
        return MOTHERDUCK_CONFIG_OPTIONS.index(key)
    return -1


def _is_md(kv):
    return kv[0].replace("motherduck_", "") in MOTHERDUCK_CONFIG_OPTIONS


def _get_md_config(config):
    return dict(sorted(filter(_is_md, config.items()), key=_get_index))


def _with_prefix(key):
    _key = key.replace("motherduck_", "")
    return f"motherduck_{_key}"


def _config_from_path(path):
    return {key: value[0] for key, value in parse_qs(urlparse(path).query).items()}


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config

    def configure_connection(self, conn: DuckDBPyConnection):
        conn.load_extension(MOTHERDUCK_EXT)
        # If a MotherDuck database is in attachments,
        # set config options *before* attaching
        if self.creds is not None and self.creds.is_motherduck_attach:
            config = {}
            # add config options specified in the path
            for attachment in self.creds.motherduck_attach:
                config.update(_config_from_path(attachment.path))
            # add config options specified via plugin config
            config.update(self._config)
            # add config options specified via settings
            if self.creds.settings is not None:
                config.update(self.creds.settings)
            # set MD config options and remove from settings
            for key, value in _get_md_config(config).items():
                conn.execute(f"SET {_with_prefix(key)} = '{value}'")
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
            for key, value in _get_md_config(self._config).items():
                config[_with_prefix(key)] = value
