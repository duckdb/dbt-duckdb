from dbt.adapters.duckdb import DuckDBConnectionManager


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def is_cancelable(cls):
        return False

