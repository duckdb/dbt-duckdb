import unittest
from argparse import Namespace
from unittest import mock

from dbt.flags import set_from_args
from dbt.adapters.duckdb import DuckDBAdapter
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.relation import DuckDBRelation
from tests.unit.utils import config_from_parts_or_dicts, mock_connection


class TestDuckDBAdapter(unittest.TestCase):
    def setUp(self):
        set_from_args(Namespace(STRICT_MODE=True), {})

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "duckdb",
                    "path": ":memory:",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg, cli_vars={})
        self._adapter = None

    @property
    def adapter(self):
        self.mock_mp_context = mock.MagicMock()
        if self._adapter is None:
            self._adapter = DuckDBAdapter(self.config, self.mock_mp_context)
        return self._adapter

    @mock.patch("dbt.adapters.duckdb.environments.duckdb")
    def test_acquire_connection(self, connector):
        connector.__version__ = "0.1.0"  # dummy placeholder for semver checks
        DuckDBConnectionManager.close_all_connections()
        connection = self.adapter.acquire_connection("dummy")

        connector.connect.assert_not_called()
        connection.handle
        self.assertEqual(connection.state, "open")
        self.assertNotEqual(connection.handle, None)
        connector.connect.assert_called_once()

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_main(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("main")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)


class TestDuckDBAdapterWithSecrets(unittest.TestCase):
    def setUp(self):
        set_from_args(Namespace(STRICT_MODE=True), {})

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "duckdb",
                    "path": ":memory:",
                    "secrets": [
                        {
                            "type": "s3",
                            "key_id": "abc",
                            "secret": "xyz",
                            "region": "us-west-2"
                        }
                    ]
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg, cli_vars={})
        self._adapter = None

    @property
    def adapter(self):
        self.mock_mp_context = mock.MagicMock()
        if self._adapter is None:
            self._adapter = DuckDBAdapter(self.config, self.mock_mp_context)
        return self._adapter

    @mock.patch("dbt.adapters.duckdb.environments.duckdb")
    def test_create_secret(self, connector):
        connector.__version__ = "0.1.0"  # dummy placeholder for semver checks
        DuckDBConnectionManager.close_all_connections()
        connection = self.adapter.acquire_connection("dummy")
        assert connection.handle
        connection._handle._env.conn.execute.assert_called_with(
"""CREATE OR REPLACE SECRET _dbt_secret_1 (
    type s3,
    key_id 'abc',
    secret 'xyz',
    region 'us-west-2'
)""")


class TestDuckDBAdapterIsDucklake(unittest.TestCase):
    def setUp(self):
        set_from_args(Namespace(STRICT_MODE=True), {})

        self.base_profile_cfg = {
            "outputs": {
                "test": {
                    "type": "duckdb",
                    "path": ":memory:",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.project_cfg = project_cfg
        self.mock_mp_context = mock.MagicMock()

    def _get_adapter(self, profile_cfg):
        config = config_from_parts_or_dicts(self.project_cfg, profile_cfg, cli_vars={})
        return DuckDBAdapter(config, self.mock_mp_context)

    def test_is_ducklake_no_attach_config(self):
        """Test is_ducklake returns False when no attach configuration exists."""
        adapter = self._get_adapter(self.base_profile_cfg)
        relation = DuckDBRelation.create(database="test_db", schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertFalse(result)

    def test_is_ducklake_empty_attach_config(self):
        """Test is_ducklake returns False when attach configuration is empty."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = []
        
        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="test_db", schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertFalse(result)

    def test_is_ducklake_with_ducklake_attachment(self):
        """Test is_ducklake returns True when relation database matches ducklake attachment."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = [
            {
                "alias": "ducklake_db",
                "path": "ducklake:sqlite:storage/metadata.sqlite"
            }
        ]
        
        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="ducklake_db", schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertTrue(result)

    def test_is_ducklake_with_regular_attachment(self):
        """Test is_ducklake returns False when relation database matches non-ducklake attachment."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = [
            {
                "alias": "regular_db",
                "path": "/path/to/regular.db"
            }
        ]
        
        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="regular_db", schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertFalse(result)

    def test_is_ducklake_with_mixed_attachments(self):
        """Test is_ducklake correctly identifies ducklake among mixed attachments."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = [
            {
                "alias": "regular_db",
                "path": "/path/to/regular.db"
            },
            {
                "alias": "ducklake_db",
                "path": "ducklake:sqlite:storage/metadata.sqlite"
            },
            {
                "alias": "another_db",
                "path": "s3://another-bucket/data"
            }
        ]
        
        adapter = self._get_adapter(profile_cfg)
        
        # Test ducklake database
        ducklake_relation = DuckDBRelation.create(database="ducklake_db", schema="test_schema", identifier="test_table")
        self.assertTrue(adapter.is_ducklake(ducklake_relation))
        
        # Test regular database
        regular_relation = DuckDBRelation.create(database="regular_db", schema="test_schema", identifier="test_table")
        self.assertFalse(adapter.is_ducklake(regular_relation))
        
        # Test another non-ducklake database
        another_relation = DuckDBRelation.create(database="another_db", schema="test_schema", identifier="test_table")
        self.assertFalse(adapter.is_ducklake(another_relation))

    def test_is_ducklake_no_database_on_relation(self):
        """Test is_ducklake returns False when relation has no database."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = [
            {
                "alias": "ducklake_db",
                "path": "ducklake:sqlite:storage/metadata.sqlite"
            }
        ]
        
        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database=None, schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertFalse(result)

    def test_is_ducklake_none_relation(self):
        """Test is_ducklake returns False when relation is None."""
        adapter = self._get_adapter(self.base_profile_cfg)
        
        result = adapter.is_ducklake(None)
        
        self.assertFalse(result)

    def test_is_ducklake_attachment_missing_alias(self):
        """Test is_ducklake handles attachments missing alias gracefully."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = [
            {
                "path": "ducklake:sqlite:storage/metadata.sqlite"
                # Missing alias
            }
        ]
        
        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="test_db", schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertFalse(result)

    def test_is_ducklake_attachment_with_empty_path(self):
        """Test is_ducklake handles attachments with empty path gracefully."""
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["attach"] = [
            {
                "alias": "test_db",
                "path": ""  # Empty path instead of missing path
            }
        ]
        
        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="test_db", schema="test_schema", identifier="test_table")
        
        result = adapter.is_ducklake(relation)
        
        self.assertFalse(result)
