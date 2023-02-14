import atexit
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import List
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
class Attachment:
    # The path to the database to be attached; may be a URL
    path: str

    # The type of the attached database (defaults to duckdb, but may be supported by an extension)
    type: Optional[str] = None

    # An optional alias for the attached database
    alias: Optional[str] = None

    # Whether the attached database is read-only or read/write
    read_only: bool = False

    def to_sql(self) -> str:
        base = f"ATTACH '{self.path}'"
        if self.alias:
            base += f" AS {self.alias}"
        options = []
        if self.type:
            options.append(f"TYPE {self.type}")
        if self.read_only:
            options.append("READ_ONLY")
        if options:
            joined = ", ".join(options)
            base += f" ({joined})"
        return base


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

    # identify whether to use the default credential provider chain for AWS/GCloud
    # instead of statically defined environment variables
    use_credential_provider: Optional[str] = None

    # A list of additional databases that should be attached to the running
    # DuckDB instance to make them available for use in models; see the
    # schema for the Attachment dataclass above for what fields it can contain
    attach: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        path = data["path"]
        if duckdb.__version__ >= "0.7.0":
            if path == ":memory:":
                data["database"] = "memory"
            else:
                data["database"] = os.path.splitext(os.path.basename(path))[0]
        return data

    @property
    def type(self):
        return "duckdb"

    def _connection_keys(self):
        return ("database", "schema", "path")

    def load_settings(self) -> Dict[str, str]:
        settings = self.settings or {}
        if self.use_credential_provider:
            if self.use_credential_provider == "aws":
                settings.update(_load_aws_credentials(ttl=_get_ttl_hash()))
            else:
                raise ValueError(
                    "Unsupported value for use_credential_provider: "
                    + self.use_credential_provider
                )
        return settings


def _get_ttl_hash(seconds=300):
    """Return the same value withing `seconds` time period"""
    return round(time.time() / seconds)


@lru_cache()
def _load_aws_credentials(ttl=None) -> Dict[str, Any]:
    del ttl  # make mypy happy
    import boto3.session

    session = boto3.session.Session()

    # use STS to verify that the credentials are valid; we will
    # raise a helpful error here if they are not
    sts = session.client("sts")
    sts.get_caller_identity()

    # now extract/return them
    aws_creds = session.get_credentials().get_frozen_credentials()
    return {
        "s3_access_key_id": aws_creds.access_key,
        "s3_secret_access_key": aws_creds.secret_key,
        "s3_session_token": aws_creds.token,
        "s3_region": session.region_name,
    }


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
            raise dbt.exceptions.DbtRuntimeError(str(e))


class DuckDBConnectionWrapper:
    def __init__(self, conn, credentials):
        self._conn = conn

        # Extensions/settings need to be configured per cursor
        cursor = conn.cursor()
        for ext in credentials.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in credentials.load_settings().items():
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

                    # attach any databases that we will be using
                    if credentials.attach:
                        for entry in credentials.attach:
                            attachment = Attachment(**entry)
                            cls.CONN.execute(attachment.to_sql())

                connection.handle = DuckDBConnectionWrapper(cls.CONN.cursor(), credentials)
                connection.state = ConnectionState.OPEN
                cls.CONN_COUNT += 1

            except RuntimeError as e:
                logger.debug(
                    "Got an error when attempting to open a duckdb " "database: '{}'".format(e)
                )

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
        with cls.LOCK:
            if cls.CONN is not None:
                cls.CONN.close()
                cls.CONN = None


atexit.register(DuckDBConnectionManager.close_all_connections)
