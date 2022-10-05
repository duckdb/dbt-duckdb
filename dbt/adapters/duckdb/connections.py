import atexit
import threading
from contextlib import contextmanager
from typing import Any, Optional, Tuple

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import (
    Connection,
    AdapterRequiredConfig,
    ConnectionState,
    AdapterResponse,
)
from dbt.logger import GLOBAL_LOGGER as logger

from dataclasses import dataclass


@dataclass
class DuckDBCredentials(Credentials):
    database: str = "main"
    schema: str = "main"
    path: str = ":memory:"

    # any extensions we want to install/load (httpfs, json, etc.)
    extensions: Optional[Tuple[str, ...]] = None

    # for connecting to data in S3 via the httpfs extension
    s3_region: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_session_token: Optional[str] = None

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

        # Extensions need to be config'd per cursor for reasons
        cursor = conn.cursor()
        for ext in credentials.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        if credentials.s3_region is not None:
            cursor.execute("LOAD 'httpfs'")
            cursor.execute(f"SET s3_region = '{credentials.s3_region}'")
            if credentials.s3_access_key_id is not None:
                cursor.execute(
                    f"SET s3_access_key_id = '{credentials.s3_access_key_id}'"
                )
                cursor.execute(
                    f"SET s3_secret_access_key = '{credentials.s3_secret_access_key}'"
                )
            else:
                cursor.execute(
                    f"SET s3_session_token = '{credentials.s3_session_token}'"
                )

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

                    if credentials.s3_region is not None:
                        cls.CONN.execute("INSTALL 'httpfs'")
                        if credentials.s3_session_token is None and (
                            credentials.s3_access_key_id is None
                            or credentials.s3_secret_access_key is None
                        ):
                            raise dbt.exceptions.RuntimeException(
                                "You must specify either s3_session_token or s3_access_key_id and s3_secret_access_key"
                            )

                connection.handle = DuckDBConnectionWrapper(
                    cls.CONN.cursor(), credentials
                )
                connection.state = ConnectionState.OPEN
                cls.CONN_COUNT += 1

            except RuntimeError as e:
                logger.debug(
                    "Got an error when attempting to open a duckdb "
                    "database: '{}'".format(e)
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
            with cls.LOCK:
                cls.CONN_COUNT -= 1
                if cls.CONN_COUNT == 0:
                    cls.CONN.close()
                    cls.CONN = None

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

    @classmethod
    def close_all_connections(cls):
        with cls.LOCK:
            if cls.CONN is not None:
                cls.CONN.close()
                cls.CONN = None


atexit.register(DuckDBConnectionManager.close_all_connections)
