import tempfile
import unittest
from pathlib import Path

import duckdb

from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments.local import LocalEnvironment


def _wal_path(db_path):
    return Path(str(db_path) + ".wal")


class TestEnvironmentCheckpoint(unittest.TestCase):
    """Regression tests for https://github.com/duckdb/dbt-duckdb/issues/703.

    When dbtRunner() is invoked multiple times in the same process, the
    shared _ENV connection stays open between runs (keep_open defaults to
    true), so committed changes sit in the write-ahead log and are never
    flushed into the database file. Fresh connections that cannot replay
    the WAL (e.g. on filesystems with unreliable locking) then see stale
    data from a previous run. Closing the last handle, or the environment
    itself, must checkpoint the database first.
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

    def test_last_handle_close_checkpoints(self):
        env = LocalEnvironment(DuckDBCredentials(path=str(self.db_path)))
        handle = self._write_via_handle(env)
        # Sanity check: the committed writes live in the WAL while the
        # connection is open.
        self.assertTrue(_wal_path(self.db_path).exists())
        handle.close()
        self.assertEqual(env.handle_count, 0)
        # The connection stays open (keep_open), but the WAL must have been
        # flushed into the database file so fresh connections see the data
        # even if they cannot replay the WAL.
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
