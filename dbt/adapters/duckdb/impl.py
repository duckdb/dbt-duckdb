from dbt.adapters.duckdb import DuckDBConnectionManager
from dbt.adapters.duckdb import DuckDBRelation

from dbt.adapters.sql import SQLAdapter


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager
    Relation = DuckDBRelation

    @classmethod
    def date_function(cls):
        return "now()"

    @classmethod
    def is_cancelable(cls):
        return False
