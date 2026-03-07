# Profile Configuration

dbt-duckdb is configured via a standard dbt `profiles.yml` file. The minimal profile requires only a single field:

```yaml
default:
  outputs:
    dev:
      type: duckdb
  target: dev
```

This runs your pipeline against an **in-memory** DuckDB database that is not persisted after the run completes. This is useful for:

- Testing pipelines locally or in CI without managing a database file
- Running pipelines that operate purely on external CSV, Parquet, or JSON files

## Persisting to a file

To persist data, set the `path` field to a local file path:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
  target: dev
```

If the file does not exist, DuckDB creates it automatically. The path is relative to your `profiles.yml` file location. The `database` property is derived automatically from the filename — for `/tmp/dbt.duckdb`, the database name is `dbt`. For in-memory mode it is `memory`.

## Common profile fields

| Field | Description |
| --- | --- |
| `path` | Path to the DuckDB file. Defaults to `:memory:`. |
| `schema` | Default schema for dbt models. |
| `threads` | Number of parallel threads dbt uses. |

## Extensions, secrets, and filesystems

See [Extensions & Settings](extensions.md) for configuring DuckDB extensions, secrets manager entries, and fsspec filesystems.

## MotherDuck

See [MotherDuck](motherduck.md) for connecting to cloud-hosted DuckDB.

## Attached databases

See [Attached Databases](databases.md) for reading and writing across multiple DuckDB, SQLite, or Postgres databases.

## Plugins

See [Plugins](plugins.md) for configuring built-in plugins like `excel`, `gsheet`, and `sqlalchemy`.

## Local Python modules

In dbt-duckdb 1.6.0+, you can add directories to the Python process's `sys.path` using `module_paths`. This lets you include custom helper modules or plugin code from within your dbt project:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      module_paths:
        - lib/
        - plugins/
  target: dev
```
