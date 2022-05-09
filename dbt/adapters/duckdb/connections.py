import contextlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState, AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class DuckDBCredentials(Credentials):
    path: str
    database: Optional[str]
    schema: Optional[str]

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = "main"
        if "schema" not in data:
            data["schema"] = "main"
        return data

    @property
    def type(self):
        return "duckdb"

    def _connection_keys(self):
        return ("path", "database", "schema")


class DuckDBCursorWrapper(contextlib.AbstractContextManager):
    def __init__(self, cursor):
        self.cursor = cursor

    # forward along all non-execute() methods/attribute look ups
    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        return None

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                return self.cursor.execute(sql)
            else:
                return self.cursor.execute(sql, bindings)
        except RuntimeError as e:
            if "cannot commit - no transaction is active" in str(e):
                return
            else:
                raise e


class DuckDBConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn

    # forward along all non-cursor() methods/attribute look ups
    def __getattr__(self, name):
        return getattr(self._conn, name)

    def cursor(self):
        return DuckDBCursorWrapper(self._conn.cursor())


class DuckDBConnectionManager(SQLConnectionManager):
    TYPE = "duckdb"

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        try:
            handle = duckdb.connect(credentials.path, read_only=False)
            connection.handle = DuckDBConnectionWrapper(handle)
            connection.state = ConnectionState.OPEN
        except RuntimeError as e:
            logger.debug(
                "Got an error when attempting to open a duckdb " "database: '{}'".format(e)
            )

            connection.handle = None
            connection.state = ConnectionState.FAIL

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        pass

    @contextlib.contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield

        except RuntimeError as e:
            logger.debug("DuckDB error: {}".format(str(e)))

            try:
                self.rollback_if_open()
            except RuntimeError:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e) from e

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)
