# MotherDuck

As of dbt-duckdb 1.5.2, you can connect to a DuckDB instance running on [MotherDuck](http://www.motherduck.com) by setting your `path` to a `md:<database>` connection string — the same format used by the DuckDB CLI and Python API.

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: "md:my_database"
  target: dev
```

## Differences from local DuckDB

MotherDuck databases behave like local DuckDB databases from dbt's perspective, but there are a few limitations to be aware of:

1. MotherDuck is compatible with specific client DuckDB versions (check the [MotherDuck docs](https://motherduck.com/docs/architecture-and-capabilities#considerations-and-limitations) for the current supported version).
2. MotherDuck preloads common DuckDB extensions but does **not** support custom extensions or user-defined functions.

## DuckLake on MotherDuck

As of dbt-duckdb 1.9.6, you can connect to a [DuckLake](https://motherduck.com/blog/ducklake-motherduck/) hosted on MotherDuck. First create a DuckLake database in MotherDuck:

```sql
CREATE DATABASE my_ducklake
  (TYPE ducklake, DATA_PATH 's3://...')
```

Then configure your profile with `is_ducklake: true`:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: "md:my_ducklake"
      is_ducklake: true
  target: dev
```

The `is_ducklake: true` flag ensures that dbt-duckdb applies safe DDL operations compatible with DuckLake's implementation.

You can also attach a MotherDuck DuckLake as an additional database. See [Attached Databases](databases.md) for details.
