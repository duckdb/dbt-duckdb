## dbt-duckdb

### Installation
This plugin can be installed via pip:
```
$ pip install dbt-duckdb
```

### Configuring your profile

[DuckDB](http://duckdb) is an embedded database, similar to SQLite, but designed for OLAP-style analytics instead of OLTP. The only configuration parameter
for the profile is the `database` which should be a path to a local file that will contain the DuckDB database.
