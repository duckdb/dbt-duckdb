import importlib.util
import os
import tempfile

import duckdb

from .credentials import DuckDBCredentials
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import DbtRuntimeError


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
    def __init__(self, cursor):
        self._cursor = DuckDBCursorWrapper(cursor)

    def close(self):
        self._cursor.close()

    def cursor(self):
        return self._cursor


class Environment:
    def handle(self):
        raise NotImplementedError

    def cursor(self):
        raise NotImplementedError

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        raise NotImplementedError

    def close(self, cursor):
        raise NotImplementedError

    @classmethod
    def initialize_db(cls, creds: DuckDBCredentials):
        config = creds.config_options or {}
        conn = duckdb.connect(creds.path, read_only=False, config=config)

        # install any extensions on the connection
        if creds.extensions is not None:
            for extension in creds.extensions:
                conn.execute(f"INSTALL '{extension}'")

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
        return conn

    @classmethod
    def initialize_cursor(cls, creds, cursor):
        # Extensions/settings need to be configured per cursor
        for ext in creds.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in creds.load_settings().items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        return cursor

    @classmethod
    def run_python_job(cls, con, load_df_function, parsed_model: dict, compiled_code: str):
        identifier = parsed_model["alias"]
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        mod_file.write(compiled_code.lstrip().encode("utf-8"))
        mod_file.close()
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

    def get_binding_char(self) -> str:
        return "?"


class LocalEnvironment(Environment):
    def __init__(self, credentials: DuckDBCredentials):
        self.conn = self.initialize_db(credentials)
        self.creds = credentials

    def handle(self):
        # Extensions/settings need to be configured per cursor
        cursor = self.initialize_cursor(self.creds, self.conn.cursor())
        return DuckDBConnectionWrapper(cursor)

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        con = handle.cursor()
        ldf = lambda table_name: con.query(f"select * from {table_name}")
        self.run_python_job(con, ldf, parsed_model, compiled_code)
        return AdapterResponse(_message="OK")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()


def create(creds: DuckDBCredentials) -> Environment:
    if creds.remote:
        from .buenavista import BVEnvironment
        return BVEnvironment(creds)
    else:
        return LocalEnvironment(creds)
