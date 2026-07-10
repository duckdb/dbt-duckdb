import os
from urllib.parse import parse_qs
from urllib.parse import urlparse

from dbt_common.exceptions import DbtRuntimeError

from . import Environment
from . import RetryableCursor
from .. import credentials
from .. import utils
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection


class MotherDuckPgEndpointCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __iter__(self):
        return iter(self._cursor)

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                self._cursor.execute(sql)
            else:
                self._cursor.execute(sql, bindings)
        except Exception as e:
            raise DbtRuntimeError(str(e)) from e
        return self


class MotherDuckPgEndpointConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn

    def close(self):
        self._conn.close()

    def commit(self):
        return self._conn.commit()

    def cursor(self):
        return MotherDuckPgEndpointCursorWrapper(self._conn.cursor())

    def rollback(self):
        return self._conn.rollback()


class MotherDuckPgEndpointEnvironment(Environment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        super().__init__(credentials)
        if not self.creds.use_motherduck_postgres_endpoint:
            raise DbtRuntimeError(
                "MotherDuckPgEndpointEnvironment requires "
                "use_motherduck_postgres_endpoint"
            )

    def handle(self):
        conn = self._connect()
        try:
            self._initialize_connection(conn)
        except Exception:
            conn.close()
            raise
        return MotherDuckPgEndpointConnectionWrapper(conn)

    def _connect(self):
        psycopg = self._import_psycopg()
        return psycopg.connect(**self._connection_kwargs())

    @staticmethod
    def _import_psycopg():
        try:
            import psycopg
        except ImportError as e:
            raise DbtRuntimeError(
                "MotherDuck PostgreSQL endpoint support requires psycopg. "
                "Install dbt-duckdb with the md-postgres extra, or install "
                "psycopg[binary]."
            ) from e
        return psycopg

    def _connection_kwargs(self):
        token = self._token()
        if not token:
            raise DbtRuntimeError(
                "A MotherDuck access token is required when "
                "use_motherduck_postgres_endpoint is true"
            )

        kwargs = {
            "host": self.creds.motherduck_pg_endpoint_host,
            "port": self.creds.motherduck_pg_endpoint_port,
            "dbname": self._dbname(),
            "user": self.creds.motherduck_pg_endpoint_user,
            "password": token,
            "sslmode": self.creds.motherduck_pg_endpoint_sslmode,
        }
        if self.creds.motherduck_pg_endpoint_sslrootcert:
            kwargs["sslrootcert"] = self.creds.motherduck_pg_endpoint_sslrootcert
        return kwargs

    def _token(self):
        if self.creds.token:
            return self.creds.token

        parsed = urlparse(self.creds.path)
        query = parse_qs(parsed.query)
        for key in ("motherduck_token", "token"):
            if key in query and query[key]:
                return query[key][0]

        return os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")

    def _dbname(self):
        parsed = urlparse(self.creds.path)
        if credentials.DuckDBCredentials._is_motherduck(parsed.scheme):
            return parsed.path or "md:"
        return self.creds.database

    def _initialize_connection(self, conn):
        cursor = MotherDuckPgEndpointCursorWrapper(conn.cursor())

        if self.creds.settings:
            for key, value in self.creds.settings.items():
                cursor.execute(f"SET {key} = '{value}'")

        if self.creds.extensions is not None:
            for extension in self.creds.extensions:
                if isinstance(extension, str):
                    cursor.execute(f"INSTALL {extension}")
                    cursor.execute(f"LOAD {extension}")
                elif isinstance(extension, dict):
                    try:
                        ext = credentials.Extension(**extension)
                    except Exception as e:
                        raise DbtRuntimeError(f"Failed to parse extension: {e}") from e
                    cursor.execute(f"INSTALL {ext.name} FROM {ext.repo}")
                    cursor.execute(f"LOAD {ext.name}")

        for sql in self.creds.secrets_sql():
            cursor.execute(sql)

        if self.creds.attach:
            for attachment in self.creds.attach:
                cursor.execute(attachment.to_sql())

        cursor.close()

    def submit_python_job(
        self, handle, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        raise DbtRuntimeError(
            "Python models are not supported when "
            "use_motherduck_postgres_endpoint is true"
        )

    def load_source(self, plugin_name: str, source_config: utils.SourceConfig) -> str:
        raise DbtRuntimeError(
            "source plugins are not supported when "
            "use_motherduck_postgres_endpoint is true"
        )

    def store_relation(self, plugin_name: str, target_config: utils.TargetConfig) -> None:
        raise DbtRuntimeError(
            "store plugins are not supported when "
            "use_motherduck_postgres_endpoint is true"
        )

    def get_binding_char(self) -> str:
        return "%s"

    @classmethod
    def initialize_cursor(
        cls,
        creds: credentials.DuckDBCredentials,
        cursor,
        plugins=None,
        registered_df: dict = {},
    ):
        if creds.retries and creds.retries.query_attempts:
            return RetryableCursor(
                cursor, creds.retries.query_attempts, creds.retries.retryable_exceptions
            )
        return cursor

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @classmethod
    def cancel(cls, connection: Connection):
        pass
