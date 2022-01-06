from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.connections import DuckDBCredentials
from dbt.adapters.duckdb.impl import DuckDBAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import duckdb


Plugin = AdapterPlugin(
    adapter=DuckDBAdapter,
    credentials=DuckDBCredentials,
    include_path=duckdb.PACKAGE_PATH,
)
