import threading

import pyarrow
from dbt_common.exceptions import DbtRuntimeError
from duckdb import CatalogException

from . import Environment
from .. import credentials
from .. import utils
from ..utils import get_retry_decorator
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection


class DuckDBCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor

    # forward along all non-execute() methods/attribute look-ups
    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                return self._cursor.execute(sql)
            else:
                return self._cursor.execute(sql, bindings)
        except RuntimeError as e:
            raise DbtRuntimeError(str(e))


class DuckDBConnectionWrapper:
    def __init__(self, cursor, env):
        self._conn = env.conn
        self._cursor = DuckDBCursorWrapper(cursor)
        self._env = env

    def close(self):
        self._cursor.close()
        self._env.notify_closed()

    def cursor(self):
        return self._cursor


class LocalEnvironment(Environment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        # Set the conn attribute to None so it always exists even if
        # DB initialization fails
        super().__init__(credentials)
        self.conn = None
        self._plugins = self.initialize_plugins(credentials)
        self.handle_count = 0
        self.lock = threading.RLock()
        self._keep_open = (
            credentials.keep_open or credentials.path == ":memory:" or credentials.is_motherduck
        )
        self._REGISTERED_DF: dict = {}

    def notify_closed(self):
        with self.lock:
            self.handle_count -= 1
            if self.handle_count == 0 and not self._keep_open:
                self.close()

    def is_cancelable(cls):
        return True

    @classmethod
    def cancel(cls, connection: Connection):
        connection.handle.cursor().interrupt()

    def handle(self):
        # Extensions/settings need to be configured per cursor
        with self.lock:
            if self.conn is None:
                self.conn = self.initialize_db(self.creds, self._plugins)
            self.handle_count += 1

        cursor = self.initialize_cursor(
            self.creds, self.conn.cursor(), self._plugins, self._REGISTERED_DF
        )
        return DuckDBConnectionWrapper(cursor, self)

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        con = handle.cursor()

        def ldf(table_name):
            return con.query(f"select * from {table_name}")

        self.run_python_job(con, ldf, parsed_model["alias"], compiled_code, self.creds)
        return AdapterResponse(_message="OK")

    def load_source(self, plugin_name: str, source_config: utils.SourceConfig):
        if plugin_name not in self._plugins:
            raise Exception(
                f"Plugin {plugin_name} not found; known plugins are: "
                + ",".join(self._plugins.keys())
            )
        plugin = self._plugins[plugin_name]
        handle = self.handle()
        cursor = handle.cursor()

        # Schema creation is currently not supported by the uc_catalog duckdb extension
        if source_config.schema and plugin_name != "unity":
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {source_config.schema}")

        save_mode = source_config.get("save_mode", "overwrite")
        if save_mode in ("ignore", "error_if_exists"):
            params = [source_config.schema, source_config.identifier]
            q = """SELECT COUNT(1)
                FROM system.information_schema.tables
                WHERE table_schema = ?
                AND table_name = ?
                """
            if source_config.database:
                q += "AND table_catalog = ?"
                params.append(source_config.database)
            if cursor.execute(q, params).fetchone()[0]:
                if save_mode == "error_if_exists":
                    raise Exception(f"Source {source_config.table_name()} already exists!")
                else:
                    # Nothing to do (we ignore the existing table)
                    return
        df = plugin.load(source_config)
        assert df is not None

        materialization = source_config.meta.get(
            "materialization", plugin.default_materialization()
        )
        source_table_name = source_config.table_name()
        df_name = source_table_name.replace(".", "_") + "_df"

        cursor.register(df_name, df)

        if materialization == "view":
            # save to df instance to register on each cursor creation
            self._REGISTERED_DF[df_name] = df

        # CREATE OR REPLACE table creation is currently not supported by the uc_catalog duckdb extension
        if plugin_name != "unity":
            cursor.execute(
                f"CREATE OR REPLACE {materialization} {source_table_name} AS SELECT * FROM {df_name}"
            )

        cursor.close()
        handle.close()

    def get_arrow_dataframe(
        self, compiled_code: str, retries: int, wait_time: float
    ) -> pyarrow.lib.Table:
        """Get the arrow dataframe from the compiled code.

        :param compiled_code: Compiled code
        :param retries: Number of retries
        :param wait_time: Wait time between retries

        :returns: Arrow dataframe
        """

        @get_retry_decorator(retries, wait_time, CatalogException)
        def execute_query():
            try:
                # Get the handle and cursor
                handle = self.handle()
                cursor = handle.cursor()

                # Execute the compiled code
                df = cursor.sql(compiled_code).arrow()

                return df
            except CatalogException as e:
                # Reset the connection to refresh the catalog
                self.conn = None

                # Raise the exception to retry the operation
                raise CatalogException(
                    f"{str(e)}: failed to execute compiled code {compiled_code}"
                )

        return execute_query()

    def store_relation(self, plugin_name: str, target_config: utils.TargetConfig) -> None:
        if plugin_name not in self._plugins:
            if plugin_name.startswith("glue|"):
                from ..plugins import glue

                _, glue_db = plugin_name.split("|")
                config = (self.creds.settings or {}).copy()
                config["glue_database"] = glue_db
                self._plugins[plugin_name] = glue.Plugin(plugin_name, config)
            else:
                raise Exception(
                    f"Plugin {plugin_name} not found; known plugins are: "
                    + ",".join(self._plugins.keys())
                )
        plugin = self._plugins[plugin_name]

        handle = self.handle()
        cursor = handle.cursor()

        # Get the number of retries and the wait time for a dbt model
        retries = int(target_config.config.get("retries", 20))
        wait_time = float(target_config.config.get("wait_time", 0.05))

        # Get the arrow dataframe
        df = self.get_arrow_dataframe(
            compiled_code=target_config.config.model.compiled_code,
            retries=retries,
            wait_time=wait_time,
        )

        plugin.store(target_config, df)

        cursor.close()
        handle.close()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()
