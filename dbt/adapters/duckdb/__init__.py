from dbt.adapters.duckdb.connections import DuckdbConnectionManager
from dbt.adapters.duckdb.connections import DuckdbCredentials
from dbt.adapters.duckdb.impl import DuckdbAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import duckdb


Plugin = AdapterPlugin(
    adapter=DuckdbAdapter,
    credentials=DuckdbCredentials,
    include_path=duckdb.PACKAGE_PATH)
