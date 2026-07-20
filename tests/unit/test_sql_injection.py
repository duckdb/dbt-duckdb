import duckdb
import pytest

from dbt.adapters.duckdb.environments import Environment
from dbt.adapters.duckdb.utils import SourceConfig


class TestInitializeCursorEscaping:
    """Verify that settings keys and values are sanitized in initialize_cursor."""

    def _run_settings(self, settings: dict):
        """Helper: create an in-memory cursor and apply settings through initialize_cursor."""
        conn = duckdb.connect(":memory:")
        cursor = conn.cursor()

        # Build a minimal mock creds with only .settings, .retries, and .plugins used
        class FakeCreds:
            retries = None

        creds = FakeCreds()
        creds.settings = settings
        Environment.initialize_cursor(creds, cursor, plugins=None, registered_df={})
        return cursor, conn

    def test_value_with_single_quote_does_not_inject(self):
        """A value containing a single quote must not break out of the SQL string.

        Without escaping this would be a syntax error or injection. With escaping,
        DuckDB receives the full literal as one string and rejects it as an
        invalid memory value -- that's the safe outcome (ParserException, not
        a multi-statement injection).
        """
        with pytest.raises(Exception, match="Unknown unit for memory"):
            self._run_settings({"memory_limit": "1GB'; DROP TABLE t;--"})

    def test_key_with_double_quote_does_not_inject(self):
        """A key containing special characters must be safely quoted."""
        # A malicious key should just fail as an unknown setting, not inject SQL
        with pytest.raises(Exception):
            self._run_settings({'" ; DROP TABLE t; --': "1"})


class TestSourceConfigQuoting:
    """Verify that table_name() properly quotes identifiers."""

    def test_simple_table_name(self):
        sc = SourceConfig(
            name="test", identifier="my_table", schema="my_schema",
            database=None, meta={}, tags=[]
        )
        assert sc.table_name() == '"my_schema"."my_table"'

    def test_table_name_with_database(self):
        sc = SourceConfig(
            name="test", identifier="my_table", schema="my_schema",
            database="my_db", meta={}, tags=[]
        )
        assert sc.table_name() == '"my_db"."my_schema"."my_table"'

    def test_table_name_escapes_double_quotes(self):
        sc = SourceConfig(
            name="test", identifier='tab"le', schema='sche"ma',
            database=None, meta={}, tags=[]
        )
        assert sc.table_name() == '"sche""ma"."tab""le"'

    def test_table_name_prevents_injection(self):
        """A malicious identifier should be safely quoted, not executed."""
        sc = SourceConfig(
            name="test",
            identifier='x"; DROP TABLE users; --',
            schema="public",
            database=None,
            meta={},
            tags=[],
        )
        name = sc.table_name()
        # The entire malicious string is inside the quotes
        assert name == '"public"."x""; DROP TABLE users; --"'

    def test_quote_identifier_static(self):
        assert SourceConfig._quote_identifier("hello") == '"hello"'
        assert SourceConfig._quote_identifier('he"lo') == '"he""lo"'
