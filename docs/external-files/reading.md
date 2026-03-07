# Reading External Files

One of DuckDB's most powerful features is reading CSV, JSON, and Parquet files directly without importing them into the database first. dbt-duckdb exposes this through the `external_location` source configuration.

## Defining an external source

Set `external_location` on a source's `meta` or `config` block. The difference:

- **`meta`** — the setting is included in the output of `dbt docs generate`
- **`config`** — the setting is excluded from generated documentation

Use `meta` for options you want documented; use `config` for options you want to keep out of docs (e.g., internal paths or credentials).

```yaml
sources:
  - name: external_source
    meta:
      external_location: "s3://my-bucket/my-sources/{name}.parquet"
    tables:
      - name: source1
      - name: source2
```

The `external_location` value is treated as an [f-string](https://peps.python.org/pep-0498/), so `{name}` is replaced with the table name. A dbt model referencing `source1`:

```sql
SELECT *
FROM {{ source('external_source', 'source1') }}
```

compiles to:

```sql
SELECT *
FROM 's3://my-bucket/my-sources/source1.parquet'
```

## Overriding location per table

If a specific table deviates from the source-level pattern, set `external_location` directly on that table:

```yaml
sources:
  - name: external_source
    meta:
      external_location: "s3://my-bucket/my-sources/{name}.parquet"
    tables:
      - name: source1
      - name: source2
        config:
          external_location: "read_parquet(['s3://my-bucket/my-sources/source2a.parquet', 's3://my-bucket/my-sources/source2b.parquet'])"
```

The table-level setting takes precedence. The model referencing `source2` compiles to:

```sql
SELECT *
FROM read_parquet(['s3://my-bucket/my-sources/source2a.parquet', 's3://my-bucket/my-sources/source2b.parquet'])
```

## Using DuckDB functions as the location

The `external_location` value does not have to be a plain path — it can be any DuckDB function call. This is useful when you need to pass options to the reader:

```yaml
sources:
  - name: flights_source
    tables:
      - name: flights
        config:
          external_location: "read_csv('flights.csv', types={'FlightDate': 'DATE'}, names=['FlightDate', 'UniqueCarrier'])"
          formatter: oldstyle
```

### String formatting strategies

By default, dbt-duckdb uses Python's `str.format` (newstyle) formatting on the `external_location` string. This can conflict with dict literals like `{'FlightDate': 'DATE'}` because curly braces are interpreted as format placeholders. Use the `formatter` option to control this:

| `formatter` value | Strategy |
| --- | --- |
| `newstyle` (default) | `str.format` — `{name}` substitution |
| `oldstyle` | `%`-style — avoids conflicts with dict literals |
| `template` | `string.Template` — `$name` substitution |

## Re-running models with an in-memory database

When using `:memory:` mode, external files are only registered as DuckDB views when they are first created. Subsequent partial runs that reference those external tables can fail because the views are gone.

To fix this, add the `register_upstream_external_models` macro to your `on-run-start` hook in `dbt_project.yml`:

```yaml
on-run-start:
  - "{{ register_upstream_external_models() }}"
```

This automatically re-registers all upstream external models at the start of each run.
