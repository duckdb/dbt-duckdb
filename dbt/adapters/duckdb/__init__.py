from dbt.adapters.base import AdapterPlugin
from dbt.adapters.duckdb.connections import DuckDBConnectionManager  # noqa
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.impl import DuckDBAdapter
from dbt.include import duckdb

Plugin = AdapterPlugin(
    adapter=DuckDBAdapter,  # type: ignore
    credentials=DuckDBCredentials,
    include_path=duckdb.PACKAGE_PATH,
)
