import atexit
import threading
from contextlib import contextmanager

import dbt.exceptions
from . import environments
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.connection import Connection
from dbt.contracts.connection import ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger


class DuckDBConnectionManager(SQLConnectionManager):
    TYPE = "duckdb"
    _LOCK = threading.RLock()
    _ENV = None

    def __init__(self, profile: AdapterRequiredConfig):
        super().__init__(profile)

    @classmethod
    def env(cls) -> environments.Environment:
        with cls._LOCK:
            if not cls._ENV:
                raise Exception("DuckDBConnectionManager environment requested before creation!")
            return cls._ENV

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        with cls._LOCK:
            try:
                if not cls._ENV:
                    cls._ENV = environments.create(credentials)
                connection.handle = cls._ENV.handle()
                connection.state = ConnectionState.OPEN

            except RuntimeError as e:
                logger.debug("Got an error when attempting to connect to DuckDB: '{}'".format(e))
                connection.handle = None
                connection.state = ConnectionState.FAIL
                raise dbt.exceptions.FailedToConnectError(str(e))

            return connection

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        connection = super(SQLConnectionManager, cls).close(connection)
        return connection

    def cancel(self, connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield
        except dbt.exceptions.DbtRuntimeError:
            raise
        except RuntimeError as e:
            logger.debug("duckdb error: {}".format(str(e)))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.DbtRuntimeError(str(exc)) from exc

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
        with cls._LOCK:
            if cls._ENV is not None:
                cls._ENV = None


atexit.register(DuckDBConnectionManager.close_all_connections)
