from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection, ConnectionState, AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class DuckDBCredentials(Credentials):
    database: str = "main"
    schema: str = "main"
    path: str = ":memory:"
    extensions: List[str] = field(default_factory=list)

    @property
    def type(self):
        return "duckdb"

    def _connection_keys(self):
        return ("database", "schema", "path")


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
            connection.handle = handle
            connection.state = ConnectionState.OPEN

            if len(credentials.extensions) > 0:
                cursor = handle.cursor()
                for extension in credentials.extensions:
                    cursor.execute(f"INSTALL '{extension}'")
                    cursor.execute(f"LOAD '{extension}'")
                cursor.close()
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

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        """
        DuckDB's cursor.execute() doesn't like None (just like SQLite) (just like SQLite)
        as a bindings argument, so substitute an empty list
        """
        if not bindings:
            bindings = []

        return super().add_query(
            sql=sql,
            auto_begin=auto_begin,
            bindings=bindings,
            abridge_sql_log=abridge_sql_log,
        )

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
