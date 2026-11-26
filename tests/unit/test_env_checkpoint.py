import gc
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock

import duckdb
from dbt.flags import set_from_args

from dbt.adapters.duckdb import DuckDBAdapter
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments.local import LocalEnvironment
from tests.unit.utils import config_from_parts_or_dicts


def _wal_path(db_path):
    return Path(str(db_path) + ".wal")


class TestEnvironmentCheckpoint(unittest.TestCase):
    """Regression tests for https://github.com/duckdb/dbt-duckdb/issues/703.

    When dbtRunner() is invoked multiple times in the same process, the
    shared _ENV connection stays open between runs (keep_open defaults to
    true), so committed changes sit in the write-ahead log and are never
    flushed into the database file. Fresh connections that cannot replay
    the WAL (e.g. on filesystems with unreliable locking) then see stale
    data from a previous run. Checkpointing once at the end of each
    invocation, via DuckDBConnectionManager.cleanup_all, flushes the WAL
    so fresh connections see the latest data.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "test.duckdb"
        self._saved_env = DuckDBConnectionManager._ENV
        self.addCleanup(setattr, DuckDBConnectionManager, "_ENV", self._saved_env)
        DuckDBConnectionManager._ENV = None

    def _write_via_handle(self, env):
        handle = env.handle()
        handle.cursor().execute("CREATE TABLE t AS SELECT 'VALUE_A' AS label")
        handle.cursor().execute("INSERT INTO t VALUES ('VALUE_B')")
        return handle

    def _adapter(self):
        set_from_args(Namespace(STRICT_MODE=True), {})
        profile_cfg = {
            "outputs": {"test": {"type": "duckdb", "path": str(self.db_path)}},
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        config = config_from_parts_or_dicts(project_cfg, profile_cfg, cli_vars={})
        return DuckDBAdapter(config, mock.MagicMock())

    def test_last_handle_close_does_not_checkpoint(self):
        env = LocalEnvironment(DuckDBCredentials(path=str(self.db_path)))
        handle = self._write_via_handle(env)
        # Sanity check: the committed writes live in the WAL while the
        # connection is open.
        self.assertTrue(_wal_path(self.db_path).exists())
        handle.close()
        self.assertEqual(env.handle_count, 0)
        # Closing the last handle does not checkpoint: dbt releases each
        # thread's connection after every node, so checkpointing here
        # would run after nearly every node instead of once per
        # invocation. The WAL is untouched until the end of the run.
        self.assertTrue(_wal_path(self.db_path).exists())
        # The checkpoint itself still works when run directly.
        env.checkpoint()
        self.assertFalse(_wal_path(self.db_path).exists())

    def test_cleanup_all_checkpoints_at_end_of_invocation(self):
        adapter = self._adapter()
        adapter.acquire_connection("test")
        adapter.execute("CREATE TABLE t AS SELECT 'VALUE_A' AS label")
        adapter.execute("INSERT INTO t VALUES ('VALUE_B')")
        adapter.release_connection()
        # Releasing the connection closes the last handle without
        # checkpointing, so the WAL is still there.
        self.assertTrue(_wal_path(self.db_path).exists())
        # cleanup_all runs at the end of an invocation and checkpoints
        # once, so fresh connections see the data even if they cannot
        # replay the WAL.
        adapter.cleanup_connections()
        self.assertFalse(_wal_path(self.db_path).exists())
        conn = duckdb.connect(str(self.db_path))
        try:
            self.assertEqual(
                conn.execute("SELECT label FROM t ORDER BY label").fetchall(),
                [("VALUE_A",), ("VALUE_B",)],
            )
        finally:
            conn.close()

    def test_close_checkpoints_before_closing(self):
        env = LocalEnvironment(DuckDBCredentials(path=str(self.db_path)))
        handle = self._write_via_handle(env)
        self.assertTrue(_wal_path(self.db_path).exists())
        handle.close()
        # keep_open is false here only if the credentials say so; with the
        # default (keep_open=True) the connection stays open, so close the
        # environment explicitly and check the WAL is flushed.
        env.close()
        self.assertIsNone(env.conn)
        self.assertFalse(_wal_path(self.db_path).exists())
        conn = duckdb.connect(str(self.db_path))
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0], 2)
        finally:
            conn.close()

    def test_close_all_connections_closes_environment(self):
        env = LocalEnvironment(DuckDBCredentials(path=str(self.db_path)))
        handle = self._write_via_handle(env)
        handle.close()
        self.assertIsNotNone(env.conn)
        DuckDBConnectionManager._ENV = env
        DuckDBConnectionManager.close_all_connections()
        self.assertIsNone(DuckDBConnectionManager._ENV)
        # close_all_connections must really release the environment instead
        # of just dropping the reference and leaking the connection.
        self.assertIsNone(env.conn)
        self.assertFalse(_wal_path(self.db_path).exists())

    def test_close_all_connections_keeps_open_handles_working(self):
        env = LocalEnvironment(DuckDBCredentials(path=str(self.db_path)))
        handle = self._write_via_handle(env)
        DuckDBConnectionManager._ENV = env
        # A handle is still open: closing all connections must not break
        # it. Only the reference is dropped, as on main.
        DuckDBConnectionManager.close_all_connections()
        self.assertIsNone(DuckDBConnectionManager._ENV)
        self.assertIsNotNone(env.conn)
        self.assertEqual(handle.cursor().execute("SELECT COUNT(*) FROM t").fetchone()[0], 2)
        handle.close()
        # The environment is released when its last handle closes, which
        # closes the connection and flushes the WAL.
        del handle
        del env
        gc.collect()
        self.assertFalse(_wal_path(self.db_path).exists())

    def test_checkpoint_on_memory_database_is_safe(self):
        env = LocalEnvironment(DuckDBCredentials(path=":memory:"))
        handle = env.handle()
        handle.cursor().execute("CREATE TABLE t AS SELECT 1 AS a")
        handle.close()
        # Must not raise; the in-memory database is unaffected.
        env.close()
        self.assertIsNone(env.conn)


if __name__ == "__main__":
    unittest.main()
