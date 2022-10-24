from dbt.adapters.duckdb import DuckDBConnectionManager
from dbt.adapters.sql import SQLAdapter


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager

    @classmethod
    def date_function(cls):
        return "now()"

    @classmethod
    def is_cancelable(cls):
        return False

    def valid_incremental_strategies(self):
        """DuckDB does not currently support MERGE statement."""
        return ["append", "delete+insert"]
