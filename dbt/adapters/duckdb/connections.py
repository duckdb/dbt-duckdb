from contextlib import contextmanager
from typing import Any, Optional, Tuple
import time

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger

from dataclasses import dataclass

@dataclass
class DuckDBCredentials(Credentials):
    database: str = 'main'
    schema: str = 'main'
    path: str = ':memory:'

    @property
    def type(self):
        return 'duckdb'

    def _connection_keys(self):
        return ('database', 'schema', 'path')


class DuckDBConnectionManager(SQLConnectionManager):
    TYPE = 'duckdb'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        try:
            handle = duckdb.connect(credentials.path, read_only=False)
            connection.handle = handle
            connection.state = 'open'
        except RuntimeError as e:
            logger.debug("Got an error when attempting to open a duckdb "
                         "database: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        # Work around error that is thrown with None bindings in
        # duckdb/sqlite
        if bindings is None:
            bindings = []
        return super().add_query(sql, auto_begin, bindings, abridge_sql_log)


    @contextmanager
    def exception_handler(self, sql: str, connection_name='master'):
        try:
            yield
        except dbt.exceptions.RuntimeException as dbte:
            raise
        except RuntimeError as e:
            logger.debug('duckdb error: {}'.format(str(e)))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.RuntimeException(str(exc)) from exc

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_status(cls, cursor):
        return 'OK'
