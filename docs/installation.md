# Installation

dbt-duckdb is hosted on PyPI. Install it with pip:

```bash
pip install dbt-duckdb
```

This installs dbt-duckdb along with its required dependencies.

## Version compatibility

The latest release targets:

- `dbt-core` >= 1.8.x
- `duckdb` >= 1.1.x

Newer DuckDB versions are supported as they are released — the project aims to keep up with upstream DuckDB releases promptly.

## Optional dependencies

Some features require additional packages:

| Feature | Required packages |
| --- | --- |
| `excel` plugin | `pandas`, `openpyxl` or `xlsxwriter` |
| `gsheet` plugin | `gspread`, `pandas` |
| `iceberg` plugin | `pyiceberg` (Python >= 3.10) |
| `sqlalchemy` plugin | `pandas`, `sqlalchemy`, relevant DB driver |
| `delta` plugin (experimental) | `deltalake` |
| `fsspec` filesystems | `s3fs`, `gcsfs`, `adlfs`, etc. |
| Interactive shell autocompletion | `iterfzf` |

Install any of these alongside dbt-duckdb as needed:

```bash
pip install dbt-duckdb "s3fs" "openpyxl"
```
