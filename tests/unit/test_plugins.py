import importlib
import inspect

import pytest

from dbt.adapters.duckdb.plugins import BasePlugin


def _import_plugin(module: str):
    try:
        return importlib.import_module(f"dbt.adapters.duckdb.plugins.{module}")
    except ImportError as e:
        pytest.skip(f"optional dependency unavailable for {module}: {e}")


@pytest.mark.parametrize("module", sorted(BasePlugin._BUILTIN))
def test_plugin_init_accepts_create_keywords(module):
    # BasePlugin.create calls mod.Plugin(name=..., plugin_config=..., credentials=...)
    plugin = _import_plugin(module)
    params = set(inspect.signature(plugin.Plugin.__init__).parameters)
    assert {"name", "plugin_config", "credentials"} <= params


class TestPostgresPlugin:
    def test_create_forwards_credentials(self, mocker):
        credentials = mocker.Mock()
        dsn = "postgresql://user:password@localhost:5432/mydb"

        plugin = BasePlugin.create(
            "postgres",
            config={"dsn": dsn},
            credentials=credentials,
        )

        assert plugin.name == "postgres"
        assert plugin._dsn == dsn
        assert plugin.creds is credentials

    def test_create_without_credentials(self):
        plugin = BasePlugin.create("postgres", config={"dsn": "postgresql://user@localhost/db"})

        assert plugin.creds is None
