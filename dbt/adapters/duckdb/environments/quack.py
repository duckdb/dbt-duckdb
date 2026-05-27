import duckdb
from dbt_common.exceptions import DbtRuntimeError

from . import Environment
from .. import credentials
from ..constants import DEFAULT_TEMP_SCHEMA_NAME
from .local import DuckDBConnectionWrapper
from .local import LocalEnvironment
from dbt.adapters.contracts.connection import AdapterResponse


class QuackEnvironment(LocalEnvironment):
    """Environment for connecting to a remote DuckDB server via the Quack protocol.

    Uses a local in-memory DuckDB as a driver that ATTACHes the remote server.
    All dbt operations are routed to the remote catalog via USE.
    """

    def __init__(self, credentials: credentials.DuckDBCredentials):
        self._quack_uri = credentials.path
        self._quack_database = credentials.database

        super().__init__(credentials)

        self._keep_open = True

    def _initialize_quack_db(self, creds, plugins=None):
        """Initialize the local driver DB and attach the remote Quack server."""
        # Temporarily set path to :memory: for the local driver connection,
        # then restore it so credentials.path keeps the original quack URI.
        original_path = creds.path
        creds.path = ":memory:"
        try:
            conn = Environment.initialize_db(creds, plugins)
        finally:
            creds.path = original_path

        # Attach the remote quack server
        # The secret with scope matching the URI is already created by initialize_db
        attach_options = []
        if creds.quack_disable_ssl is not None:
            attach_options.append(f"DISABLE_SSL {'true' if creds.quack_disable_ssl else 'false'}")
        options_clause = f" ({', '.join(attach_options)})" if attach_options else ""
        attach_sql = f"ATTACH '{self._quack_uri}' AS {self._quack_database}{options_clause}"
        try:
            conn.execute(attach_sql)
        except Exception as e:
            conn.close()
            raise DbtRuntimeError(
                f"Failed to attach Quack server at '{self._quack_uri}': {e}"
            ) from e

        # Set the default catalog to the remote database
        use_sql = f"USE {self._quack_database}"
        try:
            conn.execute(use_sql)
        except Exception as e:
            conn.close()
            raise DbtRuntimeError(
                f"Failed to set Quack catalog context with '{use_sql}': {e}"
            ) from e

        # Pre-create the dbt_temp schema to avoid write-write conflicts
        # when multiple threads try to create it simultaneously (same as MotherDuck)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DEFAULT_TEMP_SCHEMA_NAME}")

        return conn

    def _create_cursor(self):
        """Create a new cursor with the quack database context set.

        DuckDB cursors do not inherit the connection's search path,
        so each new cursor needs its own USE statement.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"USE {self._quack_database}")
        return cursor

    def handle(self):
        with self.lock:
            if self.conn is None:
                self.conn = self._initialize_quack_db(self.creds, self._plugins)
            self.handle_count += 1

        try:
            cursor = self._create_cursor()
        except (duckdb.IOException, duckdb.ConnectionException, OSError):
            # Connection lost — re-establish
            with self.lock:
                self.conn = self._initialize_quack_db(self.creds, self._plugins)
            cursor = self._create_cursor()

        cursor = self.initialize_cursor(self.creds, cursor, self._plugins, self._REGISTERED_DF)
        return DuckDBConnectionWrapper(cursor, self)

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        raise RuntimeError(
            "Python models are not supported over the Quack protocol. "
            "Quack connections cannot register local DataFrames on the remote server. "
            "Consider using a local DuckDB environment for Python models."
        )
