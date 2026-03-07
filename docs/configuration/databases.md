# Attaching Additional Databases

DuckDB supports [attaching additional databases](https://duckdb.org/docs/sql/statements/attach.html) so you can read and write across multiple databases in a single dbt run. Configure attached databases via the `attach` key in your profile (added in dbt-duckdb 1.4.0):

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      attach:
        - path: /tmp/other.duckdb
        - path: ./yet/another.duckdb
          alias: yet_another
        - path: s3://yep/even/this/works.duckdb
          read_only: true
        - path: sqlite.db
          type: sqlite
        - path: postgresql://username@hostname/dbname
          type: postgres
  target: dev
```

## Referencing attached databases

An attached database is referred to in dbt sources and models by:

- The **basename** of the file minus its extension: `/tmp/other.duckdb` → `other`, `s3://yep/even/this/works.duckdb` → `works`
- An **alias** you specify explicitly: the database at `./yet/another.duckdb` above is referred to as `yet_another`

## Supported database types

The `type` field accepts `duckdb` (default), `sqlite`, and `postgres`.

## DuckLake databases

To attach a local DuckLake, use the `ducklake:` prefix. To attach a MotherDuck-hosted DuckLake, use the `md:` prefix with `is_ducklake: true`:

```yaml
attach:
  - path: "ducklake:my_ducklake.ddb"
  - path: "md:my_other_ducklake"
    is_ducklake: true
```

## Arbitrary ATTACH options

As DuckDB adds new attachment options, you can pass them as a free-form `options` dictionary. This lets you use new DuckDB features without waiting for explicit support in dbt-duckdb:

```yaml
attach:
  # Standard direct fields
  - path: /tmp/db1.duckdb
    type: sqlite
    read_only: true

  # Equivalent using the options dict
  - path: /tmp/db2.duckdb
    options:
      type: sqlite
      read_only: true

  # Mix of direct fields and options dict (no conflicts allowed)
  - path: /tmp/db3.duckdb
    type: sqlite
    options:
      block_size: 16384

  # Future or advanced options
  - path: /tmp/db4.duckdb
    options:
      type: duckdb
      compression: lz4
      memory_limit: 2GB
```

> **Note:** If you specify the same option in both a direct field (`type`, `secret`, `read_only`) and in the `options` dict, dbt-duckdb raises an error to prevent conflicts.
