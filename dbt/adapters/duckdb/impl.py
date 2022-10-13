from dbt.adapters.base.meta import available
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

    @available
    def transpile(self, sql: str) -> str:
        return self.connections.transpile(sql)
