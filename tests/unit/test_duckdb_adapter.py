import unittest
from argparse import Namespace
from unittest import mock

from dbt.flags import set_from_args
from dbt.adapters.duckdb import DuckDBAdapter
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
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
        connection.handle._conn.execute.assert_called_with(
"""CREATE OR REPLACE SECRET __default_s3 (
    type s3,
    key_id 'abc',
    secret 'xyz',
    region 'us-west-2'
)""")
