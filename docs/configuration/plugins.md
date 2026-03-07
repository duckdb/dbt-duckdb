# Built-in Plugins

dbt-duckdb has a plugin system that lets advanced users extend its functionality. Configure plugins via the `plugins` key in your profile:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      plugins:
        - module: gsheet
          config:
            method: oauth
        - module: sqlalchemy
          alias: sql
          config:
            connection_url: "{{ env_var('DBT_ENV_SECRET_SQLALCHEMY_URI') }}"
        - module: path.to.custom_udf_module
  target: dev
```

## Plugin configuration

Each plugin entry supports:

| Field | Description |
| --- | --- |
| `module` | Required. The module that defines the `Plugin` class. Built-in plugins can be referenced by their short name (e.g., `excel`, `gsheet`). Custom plugins use their full module path. |
| `alias` | Optional. A name used for logging and reference. Defaults to the module name. |
| `config` | Optional. An arbitrary key-value dictionary passed to the plugin's `initialize` method. |

## Available built-in plugins

| Plugin | Description | Extra dependencies |
| --- | --- | --- |
| `excel` | Load source data from Excel files | `pandas`, `openpyxl` or `xlsxwriter` |
| `gsheet` | Load source data from Google Sheets | `gspread`, `pandas` |
| `sqlalchemy` | Load sources from or store models to any SQLAlchemy-compatible database | `pandas`, `sqlalchemy`, DB driver |
| `iceberg` | Read Apache Iceberg tables as sources | `pyiceberg`, Python >= 3.10 |
| `glue` | Register external materialization outputs with AWS Glue Catalog | AWS credentials |
| `delta` (experimental) | Read Delta Lake tables as sources | `deltalake` |

> **Experimental features** may change in future releases. Feedback on configuration options and use cases is welcome.

## Writing your own plugin

See [Custom Plugins](../custom-plugins.md) for how to implement a `Plugin` class and register it with dbt-duckdb.
