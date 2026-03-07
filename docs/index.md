# dbt-duckdb

<div style="display: flex; align-items: center; justify-content: center; gap: 2.5rem; padding: 1.5rem 0 2rem;">
  <img src="_assets/dbt-lightmode.svg#only-light" alt="dbt" style="height: 80px;">
  <img src="_assets/dbt-darkmode.svg#only-dark" alt="dbt" style="height: 80px;">
  <span style="font-size: 1.5rem;">+</span>
  <img src="_assets/duckdb-lightmode.svg#only-light" alt="DuckDB" style="height: 64px;">
  <img src="_assets/duckdb-darkmode.svg#only-dark" alt="DuckDB" style="height: 64px;">
</div>

[DuckDB](http://duckdb.org) is an embedded database, similar to SQLite, but designed for OLAP-style analytics. It is extremely fast and allows you to read and write data stored in CSV, JSON, and Parquet files directly, without requiring you to load them into the database first.

[dbt](http://getdbt.com) is the best way to manage a collection of data transformations written in SQL or Python for analytics and data science. `dbt-duckdb` is the project that ties DuckDB and dbt together, allowing you to create a [Modern Data Stack In A Box](https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html) or a simple and powerful data lakehouse with Python.

## Features

- **In-memory or file-backed** — run pipelines ephemerally in CI or persist to a local `.duckdb` file
- **External file support** — read and write CSV, JSON, and Parquet directly without importing
- **MotherDuck** — connect to cloud-hosted DuckDB instances
- **DuckLake** — use DuckDB's lakehouse format locally or on MotherDuck
- **Incremental models** — `append`, `delete+insert`, `merge`, and `microbatch` strategies
- **Python models** — run Python transformations in-process alongside SQL models
- **Plugin system** — extend dbt-duckdb with custom UDFs, source loaders, and external writers
- **Interactive shell** — integrated CLI with the DuckDB UI for exploratory development

## Quick start

```bash
pip install dbt-duckdb
```

Add a minimal profile to `~/.dbt/profiles.yml`:

```yaml
default:
  outputs:
    dev:
      type: duckdb
  target: dev
```

Then run your project:

```bash
dbt run
```

See [Installation](installation.md) and [Configuration](configuration/index.md) for full details.
