import abc
import importlib.util
import os
import sys
import tempfile
import time
import traceback
from typing import Dict
from typing import List
from typing import Optional

import duckdb
from dbt_common.exceptions import DbtRuntimeError

from ..constants import DEFAULT_TEMP_SCHEMA_NAME
from ..credentials import DuckDBCredentials
from ..credentials import Extension
from ..plugins import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection


def _ensure_event_loop():
    """
    Ensures the current thread has an event loop defined, and creates one if necessary.
    """
    import asyncio

    try:
        # Check that the current thread has an event loop attached
        loop = asyncio.get_event_loop()
    except RuntimeError:
        # If the current thread doesn't have an event loop, create one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)


class RetryableCursor:
    def __init__(self, cursor, retry_attempts: int, retryable_exceptions: List[str]):
        self._cursor = cursor
        self._retry_attempts = retry_attempts
        self._retryable_exceptions = retryable_exceptions

    def execute(self, sql: str, bindings=None):
        attempt, success, exc = 0, False, None
        while not success and attempt < self._retry_attempts:
            try:
                if bindings is None:
                    self._cursor.execute(sql)
                else:
                    self._cursor.execute(sql, bindings)
                success = True
            except Exception as e:
                exception_name = type(e).__name__
                if exception_name in self._retryable_exceptions:
                    time.sleep(2**attempt)
                    exc = e
                    attempt += 1
                else:
                    print(f"Did not retry exception named '{exception_name}'")
                    raise e
        if not success:
            if exc:
                raise exc
            else:
                raise RuntimeError(
                    "execute call failed, but no exceptions raised- this should be impossible"
                )
        return self

    # forward along all non-execute() methods/attribute look-ups
    def __getattr__(self, name):
        return getattr(self._cursor, name)


class Environment(abc.ABC):
    """An Environment is an abstraction to describe *where* the code you execute in your dbt-duckdb project
    actually runs. This could be the local Python process that runs dbt (which is the default),
    a remote server (like a Buena Vista instance), or even a Jupyter notebook kernel.
    """

    def __init__(self, creds: DuckDBCredentials):
        self._creds = creds

        # Add any module paths to the Python path for the environment
        if creds.module_paths:
            for path in creds.module_paths:
                if path not in sys.path:
                    sys.path.append(path)

    @property
    def creds(self) -> DuckDBCredentials:
        return self._creds

    @abc.abstractmethod
    def handle(self):
        pass

    @abc.abstractmethod
    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        pass

    @abc.abstractmethod
    def load_source(self, plugin_name: str, source_config: SourceConfig) -> str:
        pass

    @abc.abstractmethod
    def store_relation(self, plugin_name: str, target_config: TargetConfig) -> None:
        pass

    def get_binding_char(self) -> str:
        return "?"

    @classmethod
    @abc.abstractmethod
    def is_cancelable(cls) -> bool:
        pass

    @classmethod
    @abc.abstractmethod
    def cancel(cls, connection: Connection):
        pass

    @classmethod
    def initialize_db(
        cls, creds: DuckDBCredentials, plugins: Optional[Dict[str, BasePlugin]] = None
    ):
        config = creds.config_options or {}
        plugins = plugins or {}
        for plugin in plugins.values():
            plugin.update_connection_config(creds, config)

        if creds.retries:
            success, attempt, exc = False, 0, None
            while not success and attempt < creds.retries.connect_attempts:
                try:
                    conn = duckdb.connect(creds.path, read_only=False, config=config)
                    success = True
                except Exception as e:
                    exception_name = type(e).__name__
                    if exception_name in creds.retries.retryable_exceptions:
                        time.sleep(2**attempt)
                        exc = e
                        attempt += 1
                    else:
                        print(f"Did not retry exception named '{exception_name}'")
                        raise e
            if not success:
                if exc:
                    raise exc
                else:
                    raise RuntimeError(
                        "connect call failed, but no exceptions raised- this should be impossible"
                    )

        else:
            conn = duckdb.connect(creds.path, read_only=False, config=config)

        # install any extensions on the connection
        if creds.extensions is not None:
            for extension in creds.extensions:
                if isinstance(extension, str):
                    conn.install_extension(extension)
                    conn.load_extension(extension)
                elif isinstance(extension, dict):
                    try:
                        ext = Extension(**extension)
                    except Exception as e:
                        raise DbtRuntimeError(f"Failed to parse extension: {e}")
                    conn.execute(f"install {ext.name} from {ext.repo}")
                    conn.load_extension(ext.name)

        # Create/update secrets on the database
        for sql in creds.secrets_sql():
            conn.execute(sql)

        # Attach any fsspec filesystems on the database
        if creds.filesystems:
            import fsspec

            for spec in creds.filesystems:
                curr = spec.copy()
                fsimpl = curr.pop("fs")
                fs = fsspec.filesystem(fsimpl, **curr)
                conn.register_filesystem(fs)

        # let the plugins do any configuration on the
        # connection that they need to do
        if plugins:
            for plugin in plugins.values():
                plugin.configure_connection(conn)

        # attach any databases that we will be using
        if creds.attach:
            for attachment in creds.attach:
                conn.execute(attachment.to_sql())

        if creds.is_motherduck:
            # Each incremental model will try to create a temporary schema, usually the
            # DEFAULT_TEMP_SCHEMA_NAME, in its own transaction, which will result in all
            # except the first-run model to fail with a write-write conflict. By creating
            # the schema here, we make the CREATE SCHEMA statement in the incremental models
            # a no-op, which will prevent the write-write conflict.
            conn.execute("CREATE SCHEMA IF NOT EXISTS {}".format(DEFAULT_TEMP_SCHEMA_NAME))

        return conn

    @classmethod
    def initialize_cursor(
        cls,
        creds: DuckDBCredentials,
        cursor,
        plugins: Optional[Dict[str, BasePlugin]] = None,
        registered_df: dict = {},
    ):
        if creds.settings is not None:
            for key, value in creds.settings.items():
                # Okay to set these as strings because DuckDB will cast them
                # to the correct type
                cursor.execute(f"SET {key} = '{value}'")

        # update cursor if something is lost in the copy
        # of the parent connection
        if plugins:
            for plugin in plugins.values():
                plugin.configure_cursor(cursor)

        for df_name, df in registered_df.items():
            cursor.register(df_name, df)

        if creds.retries and creds.retries.query_attempts:
            cursor = RetryableCursor(
                cursor, creds.retries.query_attempts, creds.retries.retryable_exceptions
            )

        return cursor

    @classmethod
    def initialize_plugins(cls, creds: DuckDBCredentials) -> Dict[str, BasePlugin]:
        ret = {}
        base_config = creds.settings or {}
        for plugin_def in creds.plugins or []:
            config = base_config.copy()
            config.update(plugin_def.config or {})
            plugin = BasePlugin.create(
                plugin_def.module, config=config, alias=plugin_def.alias, credentials=creds
            )
            ret[plugin.name] = plugin
        return ret

    @classmethod
    def run_python_job(
        cls, con, load_df_function, identifier: str, compiled_code: str, creds: DuckDBCredentials
    ):
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        mod_file.write(compiled_code.lstrip().encode("utf-8"))
        mod_file.close()

        # Ensure that we have an event loop for async code to use since we may
        # be running inside of a thread that doesn't have one defined
        _ensure_event_loop()

        try:
            spec = importlib.util.spec_from_file_location(identifier, mod_file.name)
            if not spec:
                raise DbtRuntimeError(
                    "Failed to load python model as module: {}".format(identifier)
                )
            module = importlib.util.module_from_spec(spec)
            if spec.loader:
                spec.loader.exec_module(module)
            else:
                raise DbtRuntimeError(
                    "Python module spec is missing loader: {}".format(identifier)
                )

            # Do the actual work to run the code here
            dbt = module.dbtObj(load_df_function)
            df = module.model(dbt, con)
            if isinstance(df, duckdb.DuckDBPyRelation):
                # a duckdb relation might contain references to temporary tables
                # that cannot cross cursor boundaries
                module.materialize(df, con)
            else:
                # Create a separate read cursor to enable batched reads/writes
                cur = cls.initialize_cursor(creds, con.cursor())
                module.materialize(df, cur)
        except Exception as err:
            raise DbtRuntimeError(
                f"Python model failed:\n" f"{''.join(traceback.format_exception(err))}"
            )
        finally:
            os.unlink(mod_file.name)


def create(creds: DuckDBCredentials) -> Environment:
    """Create an Environment based on the credentials passed in."""

    if creds.remote:
        from .buenavista import BVEnvironment

        return BVEnvironment(creds)
    elif creds.is_motherduck:
        from .motherduck import MotherDuckEnvironment

        return MotherDuckEnvironment(creds)
    else:
        from .local import LocalEnvironment

        return LocalEnvironment(creds)
