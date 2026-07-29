"""Tests for single-quote escaping across attach, secrets, and settings."""

from unittest.mock import MagicMock

import duckdb

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
from dbt.adapters.duckdb.utils import escape_sql_string


# -- helper ------------------------------------------------------------------


class TestEscapeSqlString:
    def test_no_quotes(self):
        assert escape_sql_string("hello") == "hello"

    def test_single_quote(self):
        assert escape_sql_string("it's") == "it''s"

    def test_multiple_quotes(self):
        assert escape_sql_string("a'b'c") == "a''b''c"

    def test_non_string(self):
        assert escape_sql_string(42) == "42"


# -- Attachment ---------------------------------------------------------------


class TestAttachmentEscaping:
    def test_path_with_single_quote(self):
        a = Attachment(path="/tmp/it's.db")
        sql = a.to_sql()
        assert "it''s.db" in sql
        assert "ATTACH IF NOT EXISTS '/tmp/it''s.db'" in sql

    def test_option_value_with_quote(self):
        a = Attachment(path="/tmp/test.db", options={"catalog": "it's_cat"})
        sql = a.to_sql()
        assert "it''s_cat" in sql


# -- Secret -------------------------------------------------------------------


class TestSecretEscaping:
    def test_scalar_value_with_quote(self):
        s = Secret.create("s3", key_id="it's_key")
        sql = s.to_sql()
        assert "it''s_key" in sql

    def test_map_value_with_quote(self):
        s = Secret.create(
            "s3",
            extra_http_headers={"X-Header": "val'ue"},
        )
        sql = s.to_sql()
        assert "val''ue" in sql

    def test_array_value_with_quote(self):
        s = Secret.create("s3", scope=["s3://it's_bucket"])
        sql = s.to_sql()
        assert "it''s_bucket" in sql

    def test_scope_string_with_quote(self):
        s = Secret.create("s3", scope="s3://it's_bucket")
        sql = s.to_sql()
        assert "it''s_bucket" in sql


# -- Settings via initialize_cursor ------------------------------------------


class TestSettingsEscaping:
    def test_setting_value_with_quote(self):
        """Mock cursor captures the SQL and verifies escaping."""
        from dbt.adapters.duckdb.environments import Environment

        mock_creds = MagicMock()
        mock_creds.settings = {"custom_key": "val'ue"}
        mock_creds.retries = None

        mock_cursor = MagicMock()
        Environment.initialize_cursor(mock_creds, mock_cursor, plugins=None)

        mock_cursor.execute.assert_called_once_with("SET custom_key = 'val''ue'")


# -- Live DuckDB round-trip --------------------------------------------------


class TestLiveDuckDBRoundTrip:
    def test_setting_roundtrip(self):
        """Feed escaped SQL to a real DuckDB and read the value back."""
        conn = duckdb.connect()
        value_with_quote = "it's_a_test"
        escaped = escape_sql_string(value_with_quote)
        conn.execute(f"SET file_search_path = '{escaped}'")
        result = conn.execute("SELECT current_setting('file_search_path')").fetchone()[0]
        assert result == value_with_quote
        conn.close()
