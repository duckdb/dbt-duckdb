import atexit
import threading
from contextlib import contextmanager
from multiprocessing.context import SpawnContext
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TYPE_CHECKING

import dbt.exceptions
from . import environments
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager

logger = AdapterLogger("DuckDB")

if TYPE_CHECKING:
    import agate


class DuckDBConnectionManager(SQLConnectionManager):
    TYPE = "duckdb"
    _LOCK = threading.RLock()
    _ENV = None
    _LOGGED_MESSAGES: Set[str] = set()

    def __init__(self, config: AdapterRequiredConfig, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        self.disable_transactions = config.credentials.disable_transactions  # type: ignore

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
                if not cls._ENV or cls._ENV.creds != credentials:
                    cls._ENV = environments.create(credentials)
                connection.handle = cls._ENV.handle()
                connection.state = ConnectionState.OPEN

            except RuntimeError as e:
                logger.debug("Got an error when attempting to connect to DuckDB: '{}'".format(e))
                connection.handle = None
                connection.state = ConnectionState.FAIL
                raise dbt.adapters.exceptions.FailedToConnectError(str(e))

            return connection

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        connection = super(SQLConnectionManager, cls).close(connection)
        return connection

    @classmethod
    def warn_once(cls, msg: str):
        """Post a warning message once per dbt execution."""
        with cls._LOCK:
            if msg in cls._LOGGED_MESSAGES:
                return
            cls._LOGGED_MESSAGES.add(msg)
            logger.warning(msg)

    def cancel(self, connection: Connection):
        if self._ENV is not None:
            logger.debug(
                "cancelling query on connection {}. Details: {}".format(
                    connection.name, connection
                )
            )
            self._ENV.cancel(connection)
            logger.debug("query cancelled on connection {}".format(connection.name))

    @contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield
        except dbt.exceptions.DbtRuntimeError:
            raise
        except RuntimeError as e:
            logger.debug("duckdb error: {}".format(str(e)))
            logger.debug("Error running SQL: {}".format(sql))
            # Preserve original RuntimeError with full context instead of swallowing
            raise dbt.exceptions.DbtRuntimeError(str(e)) from e
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

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        if self.disable_transactions:
            auto_begin = False
        return super().execute(sql, auto_begin, fetch, limit)


atexit.register(DuckDBConnectionManager.close_all_connections)
