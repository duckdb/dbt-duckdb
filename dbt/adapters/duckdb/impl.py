from typing import Sequence

from dbt.adapters.base.meta import available
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import InternalException
from dbt.exceptions import RuntimeException


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @available
    def location_exists(self, location: str) -> bool:
        try:
            self.execute(
                f"select 1 from '{location}' where 1=0",
                auto_begin=False,
                fetch=False,
            )
            return True
        except RuntimeException:
            return False

    def valid_incremental_strategies(self) -> Sequence[str]:
        """DuckDB does not currently support MERGE statement."""
        return ["append", "delete+insert"]

    def commit_if_has_connection(self) -> None:
        """This is just a quick-fix. Python models do not execute begin function so the transaction_open is always false."""
        try:
            self.connections.commit_if_has_connection()
        except InternalException:
            pass

    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:

        connection = self.connections.get_if_exists()
        if not connection:
            connection = self.connections.get_thread_connection()
        con = connection.handle._conn

        def load_df_function(table_name: str):
            """
            Currently con.table method dos not support fully qualified name - https://github.com/duckdb/duckdb/issues/5038

            Can be replaced by con.table, after it is fixed.
            """
            return con.query(f"select * from {table_name}")

        try:
            exec(compiled_code, {}, {"load_df_function": load_df_function, "con": con})
        except SyntaxError as err:
            raise RuntimeException(
                f"Python model has a syntactic error at line {err.lineno}:\n" f"{err}\n"
            )
        except Exception as err:
            raise RuntimeException(f"Python model failed:\n" f"{err}")
        return AdapterResponse(_message="OK")
