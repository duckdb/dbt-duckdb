"""Tests for SQL-injection hardening in Secret.to_sql()."""

import pytest

from dbt.adapters.duckdb.secrets import Secret


def _make_secret(**kwargs):
    defaults = {"type": "s3"}
    defaults.update(kwargs)
    secret_type = defaults.pop("type")
    name = defaults.pop("name", None)
    provider = defaults.pop("provider", None)
    scope = defaults.pop("scope", None)
    persistent = defaults.pop("persistent", None)
    return Secret.create(
        secret_type=secret_type,
        name=name,
        provider=provider,
        scope=scope,
        persistent=persistent,
        **defaults,
    )


class TestEscapeStringValues:
    def test_plain_value_unchanged(self):
        s = _make_secret(key_id="AKIAEXAMPLE")
        sql = s.to_sql()
        assert "key_id 'AKIAEXAMPLE'" in sql

    def test_single_quote_in_value_is_escaped(self):
        s = _make_secret(key_id="it's")
        sql = s.to_sql()
        assert "key_id 'it''s'" in sql

    def test_multiple_quotes_escaped(self):
        s = _make_secret(key_id="a'b'c")
        sql = s.to_sql()
        assert "key_id 'a''b''c'" in sql

    def test_injection_payload_neutralized(self):
        payload = "'); DROP TABLE secrets; --"
        s = _make_secret(key_id=payload)
        sql = s.to_sql()
        # The payload must appear fully inside a quoted string, not break out
        assert "DROP TABLE" in sql  # still present as data
        assert "key_id '''); DROP TABLE secrets; --'" in sql


class TestEscapeMapValues:
    def test_map_value_with_quote(self):
        s = _make_secret(extra_http_headers={"X-Key": "val'ue"})
        sql = s.to_sql()
        assert "'val''ue'" in sql

    def test_map_key_with_quote(self):
        s = _make_secret(extra_http_headers={"X-K'ey": "value"})
        sql = s.to_sql()
        assert "'X-K''ey'" in sql


class TestEscapeListValues:
    def test_list_item_with_quote(self):
        s = _make_secret(scope=["s3://bucket/it's"])
        sql = s.to_sql()
        assert "'s3://bucket/it''s'" in sql


class TestSecretNameValidation:
    def test_valid_name(self):
        s = _make_secret(name="my_secret_1")
        sql = s.to_sql()
        assert "SECRET my_secret_1" in sql

    def test_name_with_injection_raises(self):
        s = _make_secret(name="x; DROP TABLE t; --")
        with pytest.raises(ValueError, match="Invalid secret name"):
            s.to_sql()

    def test_name_with_quote_raises(self):
        s = _make_secret(name="secret'name")
        with pytest.raises(ValueError, match="Invalid secret name"):
            s.to_sql()

    def test_no_name_is_fine(self):
        s = _make_secret()
        sql = s.to_sql()
        assert "CREATE SECRET" in sql
        assert "OR REPLACE" not in sql
