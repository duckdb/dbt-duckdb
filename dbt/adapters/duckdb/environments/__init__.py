import abc
import importlib.util
import os
import sys
import tempfile
from typing import Dict
from typing import Optional

import duckdb

from ..credentials import DuckDBCredentials
from ..plugins import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import DbtRuntimeError


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
    def initialize_db(
        cls, creds: DuckDBCredentials, plugins: Optional[Dict[str, BasePlugin]] = None
    ):
        config = creds.config_options or {}
        conn = duckdb.connect(creds.path, read_only=False, config=config)

        # install any extensions on the connection
        if creds.extensions is not None:
            for extension in creds.extensions:
                conn.install_extension(extension)
                conn.load_extension(extension)

        # Attach any fsspec filesystems on the database
        if creds.filesystems:
            import fsspec

            for spec in creds.filesystems:
                curr = spec.copy()
                fsimpl = curr.pop("fs")
                fs = fsspec.filesystem(fsimpl, **curr)
                conn.register_filesystem(fs)

        # attach any databases that we will be using
        if creds.attach:
            for attachment in creds.attach:
                conn.execute(attachment.to_sql())

        # let the plugins do any configuration on the
        # connection that they need to do
        if plugins:
            for plugin in plugins.values():
                plugin.configure_connection(conn)

        return conn

    @classmethod
    def initialize_cursor(cls, creds: DuckDBCredentials, cursor):
        for key, value in creds.load_settings().items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        return cursor

    @classmethod
    def initialize_plugins(cls, creds: DuckDBCredentials) -> Dict[str, BasePlugin]:
        ret = {}
        base_config = creds.settings or {}
        for plugin_def in creds.plugins or []:
            config = base_config.copy()
            config.update(plugin_def.config or {})
            plugin = BasePlugin.create(plugin_def.module, config=config, alias=plugin_def.alias)
            ret[plugin.name] = plugin
        return ret

    @classmethod
    def run_python_job(cls, con, load_df_function, identifier: str, compiled_code: str):
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
            module.materialize(df, con)
        except Exception as err:
            raise DbtRuntimeError(f"Python model failed:\n" f"{err}")
        finally:
            os.unlink(mod_file.name)


def create(creds: DuckDBCredentials) -> Environment:
    """Create an Environment based on the credentials passed in."""

    if creds.remote:
        from .buenavista import BVEnvironment

        return BVEnvironment(creds)
    else:
        from .local import LocalEnvironment

        return LocalEnvironment(creds)
