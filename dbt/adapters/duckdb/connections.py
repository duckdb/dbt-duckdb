import atexit
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.connection import Connection
from dbt.contracts.connection import ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class DuckDBCredentials(Credentials):
    database: str = "main"
    schema: str = "main"
    path: str = ":memory:"

    # any extensions we want to install and load (httpfs, parquet, etc.)
    extensions: Optional[Tuple[str, ...]] = None

    # any additional pragmas we want to configure on our DuckDB connections;
    # a list of the built-in pragmas can be found here:
    # https://duckdb.org/docs/sql/configuration
    # (and extensions may add their own pragmas as well)
    settings: Optional[Dict[str, Any]] = None

    # the root path to use for any external materializations that are specified
    # in this dbt project; defaults to "." (the current working directory)
    external_root: str = "."

    @property
    def type(self):
        return "duckdb"

    def _connection_keys(self):
        return ("database", "schema", "path")


class DuckDBCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor

    # forward along all non-execute() methods/attribute look ups
    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                return self._cursor.execute(sql)
            else:
                return self._cursor.execute(sql, bindings)
        except RuntimeError as e:
            raise dbt.exceptions.RuntimeException(str(e))


class DuckDBConnectionWrapper:
    def __init__(self, conn, credentials):
        self._conn = conn

        # Extensions/settings need to be configured per cursor
        cursor = conn.cursor()
        for ext in credentials.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in (credentials.settings or {}).items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        self._cursor = DuckDBCursorWrapper(cursor)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def cursor(self):
        return self._cursor


class DuckDBConnectionManager(SQLConnectionManager):
    TYPE = "duckdb"
    LOCK = threading.RLock()
    CONN = None
    CONN_COUNT = 0

    def __init__(self, profile: AdapterRequiredConfig):
        super().__init__(profile)

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        with cls.LOCK:
            try:
                if not cls.CONN:
                    cls.CONN = duckdb.connect(credentials.path, read_only=False)

                    # install any extensions on the connection
                    if credentials.extensions is not None:
                        for extension in credentials.extensions:
                            cls.CONN.execute(f"INSTALL '{extension}'")

                connection.handle = DuckDBConnectionWrapper(cls.CONN.cursor(), credentials)
                connection.state = ConnectionState.OPEN
                cls.CONN_COUNT += 1

            except RuntimeError as e:
                logger.debug(
                    "Got an error when attempting to open a duckdb " "database: '{}'".format(e)
                )

                connection.handle = None
                connection.state = ConnectionState.FAIL

                raise dbt.exceptions.FailedToConnectException(str(e))
            return connection

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        connection = super(SQLConnectionManager, cls).close(connection)

        if connection.state == ConnectionState.CLOSED:
            credentials = cls.get_credentials(connection.credentials)
            with cls.LOCK:
                cls.CONN_COUNT -= 1
                if cls.CONN_COUNT == 0 and cls.CONN and not credentials.path == ":memory:":
                    cls.CONN.close()
                    cls.CONN = None

        return connection

    def cancel(self, connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield
        except dbt.exceptions.RuntimeException:
            raise
        except RuntimeError as e:
            logger.debug("duckdb error: {}".format(str(e)))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.RuntimeException(str(exc)) from exc

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)

    @classmethod
    def close_all_connections(cls):
        with cls.LOCK:
            if cls.CONN is not None:
                cls.CONN.close()
                cls.CONN = None


atexit.register(DuckDBConnectionManager.close_all_connections)
