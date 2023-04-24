from typing import List

import pyarrow as pa

from . import Environment
from .. import column
from .. import credentials
from .. import utils
from dbt.adapters.base.column import Column
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


def convert_type(t: pa.DataType) -> str:
    if pa.types.is_int64(t):
        return "BIGINT"
    elif pa.types.is_integer(t):
        return "INTEGER"
    elif pa.types.is_string(t):
        return "TEXT"
    elif pa.types.is_date(t):
        return "DATE"
    elif pa.types.is_time(t):
        return "TIME"
    elif pa.types.is_timestamp(t):
        return "DATETIME"
    elif pa.types.is_floating(t):
        return "FLOAT"
    elif pa.types.is_decimal(t):
        return "DECIMAL"
    elif pa.types.is_boolean(t):
        return "BOOL"
    elif pa.types.is_binary(t):
        return "BINARY"
    elif pa.types.is_interval(t):
        return "INTERVAL"
    elif pa.types.is_list(t):
        field_type = t.field(0).type
        if pa.types.is_integer(field_type):
            return "INTEGERARRAY"
        elif pa.types.is_string(field_type):
            return "STRINGARRAY"
        else:
            return "ARRAY"
    elif pa.types.is_struct(t) or pa.types.is_map(t):
        # TODO: support detailed nested types
        return "JSON"
    else:
        return "UNKNOWN"


class LocalEnvironment(Environment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        self.conn = self.initialize_db(credentials)
        self._plugins = self.initialize_plugins(credentials)
        self.creds = credentials

    def handle(self):
        # Extensions/settings need to be configured per cursor
        cursor = self.initialize_cursor(self.creds, self.conn.cursor())
        return DuckDBConnectionWrapper(cursor)

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        con = handle.cursor()

        def ldf(table_name):
            return con.query(f"select * from {table_name}")

        self.run_python_job(con, ldf, parsed_model["alias"], compiled_code)
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
        save_mode = source_config.meta.get("save_mode", "overwrite")
        if save_mode in ("ignore", "error_if_exists"):
            schema, identifier = source_config.schema, source_config.identifier
            q = f"""SELECT COUNT(1)
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{identifier}'
                """
            if cursor.execute(q).fetchone()[0]:
                if save_mode == "error_if_exists":
                    raise Exception(f"Source {source_config.table_name()} already exists!")
                else:
                    # Nothing to do (we ignore the existing table)
                    return
        df = plugin.load(source_config)
        assert df is not None
        materialization = source_config.meta.get("materialization", "table")
        cursor.execute(
            f"CREATE OR REPLACE {materialization} {source_config.table_name()} AS SELECT * FROM df"
        )
        cursor.close()
        handle.close()

    def create_columns(self, cursor) -> List[Column]:
        columns = []
        for field in cursor.fetch_record_batch().schema:
            columns.append(column.DuckDBColumn.create(field.name, convert_type(field.type)))
        return columns

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()
