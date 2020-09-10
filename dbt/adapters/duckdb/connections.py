from contextlib import contextmanager

import duckdb

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger

from dataclasses import dataclass

@dataclass
class DuckDBCredentials(Credentials):
    database: str = ':memory:'

    _ALIASES = {
        'dbname': 'database',
    }

    @property
    def type(self):
        return 'duckdb'

    def _connection_keys(self):
        return ('database',)

class DuckDBConnectionManager(SQLConnectionManager):
    TYPE = 'duckdb'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        try:
            handle = duckdb.connect(credentials.database, read_only=False)
            connection.handle = handle
            connection.state = 'open'
        except Error as e:
            logger.debug("Got an error when attempting to open a duckdb "
                         "database: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str, connection_name='master'):
        try:
            yield
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.release(connection_name)
            raise dbt.exceptions.RuntimeException(str(exc))

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_status(cls, cursor):
        return 'OK'
