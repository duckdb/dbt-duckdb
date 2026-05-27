"""Unit tests for Quack credential parsing and secret generation."""
import unittest

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.secrets import Secret


class TestQuackCredentials(unittest.TestCase):
    def test_is_quack_true(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
            "quack_token": "token123",
        })
        assert creds.is_quack is True
        assert creds.is_motherduck is False

    def test_is_quack_false_memory(self):
        creds = DuckDBCredentials.from_dict({
            "path": ":memory:",
        })
        assert creds.is_quack is False

    def test_is_quack_false_file(self):
        creds = DuckDBCredentials.from_dict({
            "path": "/tmp/test.duckdb",
            "database": "test",
        })
        assert creds.is_quack is False

    def test_is_quack_false_motherduck(self):
        creds = DuckDBCredentials.from_dict({
            "path": "md:my_db",
        })
        assert creds.is_quack is False
        assert creds.is_motherduck is True

    def test_quack_default_database_alias(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:myserver.example.com:9494",
            "quack_token": "t",
        })
        assert creds.database == "quack_duckdb"

    def test_quack_explicit_database(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "my_remote",
            "quack_token": "t",
        })
        assert creds.database == "my_remote"

    def test_quack_auto_injects_extension(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
            "quack_token": "token",
        })
        ext_names = []
        for ext in (creds.extensions or []):
            if isinstance(ext, str):
                ext_names.append(ext)
            elif isinstance(ext, dict):
                ext_names.append(ext.get("name", ""))
        assert "quack" in ext_names

    def test_quack_does_not_duplicate_extension(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
            "quack_token": "token",
            "extensions": [{"name": "quack", "repo": "core_nightly"}],
        })
        quack_count = sum(
            1 for ext in (creds.extensions or [])
            if (isinstance(ext, str) and ext == "quack")
            or (isinstance(ext, dict) and ext.get("name") == "quack")
        )
        assert quack_count == 1

    def test_quack_auto_injects_secret(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
            "quack_token": "my_token",
        })
        secret_sqls = creds.secrets_sql()
        assert len(secret_sqls) >= 1
        quack_secrets = [s for s in secret_sqls if "quack" in s and "my_token" in s]
        assert len(quack_secrets) == 1

    def test_quack_does_not_duplicate_secret(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
            "quack_token": "my_token",
            "secrets": [{"type": "quack", "token": "manual_token"}],
        })
        secret_sqls = creds.secrets_sql()
        quack_secrets = [s for s in secret_sqls if "type quack" in s]
        # Should only have 1 quack secret (the manual one), not duplicated
        assert len(quack_secrets) == 1

    def test_quack_no_token_no_secret(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
        })
        secret_sqls = creds.secrets_sql()
        quack_secrets = [s for s in secret_sqls if "type quack" in s]
        assert len(quack_secrets) == 0

    def test_quack_unique_field(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:myhost:9494",
            "database": "quack_duckdb",
            "quack_token": "t",
        })
        assert creds.unique_field == "quack:myhost:9494."

    def test_quack_connection_keys(self):
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
        })
        keys = creds._connection_keys()
        assert "quack_token" in keys
        assert "quack_disable_ssl" in keys

    def test_quack_disable_ssl_not_in_secret(self):
        """disable_ssl is an ATTACH option, not a secret parameter."""
        creds = DuckDBCredentials.from_dict({
            "path": "quack:localhost:9494",
            "database": "quack_duckdb",
            "quack_token": "tok",
            "quack_disable_ssl": True,
        })
        secret_sqls = creds.secrets_sql()
        quack_sql = [s for s in secret_sqls if "type quack" in s][0]
        assert "disable_ssl" not in quack_sql
        assert creds.quack_disable_ssl is True


class TestQuackSecretSQL(unittest.TestCase):
    def test_quack_secret_generation(self):
        secret = Secret.create(
            secret_type="quack",
            name="_dbt_quack",
            token="abc123",
            scope="quack:localhost:9494",
        )
        sql = secret.to_sql()
        assert "type quack" in sql
        assert "token 'abc123'" in sql
        assert "scope 'quack:localhost:9494'" in sql

    def test_quack_secret_with_disable_ssl(self):
        secret = Secret.create(
            secret_type="quack",
            name="_dbt_quack",
            token="abc123",
            scope="quack:localhost:9494",
            disable_ssl="true",
        )
        sql = secret.to_sql()
        assert "disable_ssl 'true'" in sql


if __name__ == "__main__":
    unittest.main()
