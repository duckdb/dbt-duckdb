import importlib
import inspect

import duckdb
import pytest

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment
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

    def test_attach_statement_escapes_quoted_dsn_values(self):
        # libpq keyword/value DSNs single-quote values that contain spaces
        dsn = "host=localhost dbname=mydb options='-c search_path=analytics'"
        plugin = BasePlugin.create(
            "postgres",
            config={
                "dsn": dsn,
                "pg_schema": "o'brien",
                "secret": "pg's_secret",
                "attach_options": {"extra": "it's"},
            },
        )

        stmt = plugin._build_attach_statement()

        assert stmt.startswith(f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS postgres_db")
        assert "SCHEMA 'o''brien'" in stmt
        assert "SECRET 'pg''s_secret'" in stmt
        assert "EXTRA 'it''s'" in stmt
        # the statement must parse, and the DSN literal must round-trip unchanged
        assert len(duckdb.extract_statements(stmt)) == 1
        literal = stmt[len("ATTACH ") : stmt.index(" AS postgres_db")]
        assert duckdb.sql(f"SELECT {literal}").fetchone()[0] == dsn

    def test_extension_setting_escapes_quotes(self, mocker):
        plugin = BasePlugin.create(
            "postgres",
            config={"dsn": "dbname=mydb", "settings": {"pg_application_name": "dbt's run"}},
        )
        conn = mocker.Mock()

        plugin._set_extension_settings(conn)

        conn.execute.assert_called_once_with("SET pg_application_name = 'dbt''s run';")


class RewritingCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, bindings=None):
        return self._cursor.execute(sql.replace("41", "42"), bindings)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class RewritingPlugin(BasePlugin):
    def modify_cursor(self, cursor):
        return RewritingCursor(cursor)


class TestModifyCursor:
    def test_default_returns_same_cursor(self):
        plugin = BasePlugin("base", {})
        cursor = object()

        assert plugin.modify_cursor(cursor) is cursor

    def test_initialize_cursor_uses_modified_cursor(self):
        creds = DuckDBCredentials()
        plugins = {"rewrite": RewritingPlugin("rewrite", {})}
        conn = duckdb.connect()

        cursor = Environment.initialize_cursor(creds, conn.cursor(), plugins)

        assert cursor.execute("SELECT 41").fetchone() == (42,)
        conn.close()
