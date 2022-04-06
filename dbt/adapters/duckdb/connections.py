from contextlib import contextmanager
from typing import Any, Optional, Tuple
import time

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection, ConnectionState, AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger

from dataclasses import dataclass


@dataclass
class DuckDBCredentials(Credentials):
    database: str = "main"
    schema: str = "main"
    path: str = ":memory:"

    @property
    def type(self):
        return "duckdb"

    def _connection_keys(self):
        return ("database", "schema", "path")


class DuckDBCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    # forward along all non-execute() methods/attribute look ups
    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                 return self.cursor.execute(sql)
            else:
                 return self.cursor.execute(sql, bindings)
        except RuntimeError as e:
            raise dbt.exceptions.RuntimeException(str(e))


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
                "Got an error when attempting to open a duckdb "
                "database: '{}'".format(e)
            )

            connection.handle = None
            connection.state = ConnectionState.FAIL

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield
        except dbt.exceptions.RuntimeException as dbte:
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
