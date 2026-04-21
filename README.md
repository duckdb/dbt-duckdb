## dbt-duckdb

[DuckDB](http://duckdb.org) is an embedded database, similar to SQLite, but designed for OLAP-style analytics. It is fast and allows you to read and write data stored in CSV, JSON, and Parquet files directly, without requiring you to load them into the database first.

[dbt](http://getdbt.com) is a framework for managing data transformations written in SQL or Python for analytics and data science. `dbt-duckdb` is the adapter that ties DuckDB and dbt together, allowing you to build a [Modern Data Stack In A Box](https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html) or a local lakehouse with Python.

### Installation

This project is hosted on PyPI, so you should be able to install it and the necessary dependencies via:

`pip3 install dbt-duckdb`

The latest supported version targets `dbt-core` versions >= 1.8.x and DuckDB version 1.1.x, but we work hard to ensure that newer versions of DuckDB continue to work with the adapter as they are released.

### Documentation

Most user-facing setup and usage documentation for `dbt-duckdb` now lives on docs.getdbt.com:

- [DuckDB setup](https://docs.getdbt.com/docs/local/connect-data-platform/duckdb-setup)
- [DuckDB configurations](https://docs.getdbt.com/reference/resource-configs/duckdb-configs)
- [Quickstart for dbt Core using DuckDB](https://docs.getdbt.com/guides/duckdb)
- [Materializations](https://docs.getdbt.com/docs/build/materializations)
- [Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [Python models](https://docs.getdbt.com/docs/build/python-models)

Use this README for repository-specific topics that are better kept close to the adapter source, especially plugins and extension points.

### Configuring dbt-duckdb plugins

`dbt-duckdb` has its own [plugin](dbt/adapters/duckdb/plugins/__init__.py) system to enable advanced users to extend the adapter with additional functionality, including:

- Defining [custom Python UDFs](https://duckdb.org/docs/api/python/function.html) on the DuckDB database connection so they can be used in SQL models
- Loading source data from [Excel](dbt/adapters/duckdb/plugins/excel.py), [Google Sheets](dbt/adapters/duckdb/plugins/gsheet.py), or [SQLAlchemy](dbt/adapters/duckdb/plugins/sqlalchemy.py) tables

To configure a plugin for use in your dbt project, use the `plugins` property in your profile:

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

Every plugin must have a `module` property that indicates where the `Plugin` class to load is defined. There is a set of built-in plugins defined in [dbt.adapters.duckdb.plugins](dbt/adapters/duckdb/plugins/) that may be referenced by their base filename, such as `excel` or `gsheet`, while user-defined plugins should be referenced by their full module path name.

Each plugin instance has a name for logging and reference purposes that defaults to the name of the module but may be overridden with the `alias` property. Modules may also be initialized using arbitrary key-value pairs in the `config` dictionary.

Using plugins may require additional dependencies in the Python environment that your dbt-duckdb pipeline runs in:

- `excel` depends on `pandas`, and `openpyxl` or `xlsxwriter` to perform writes
- `gsheet` depends on `gspread` and `pandas`
- `iceberg` depends on `pyiceberg` and Python >= 3.10
- `sqlalchemy` depends on `pandas`, `sqlalchemy`, and the driver or drivers you need

**Experimental:**

- `delta` depends on `deltalake`, and an [example project is here](https://github.com/milicevica23/dbt-duckdb-delta-plugin-demo)

**Note:** Experimental features can change over time, and feedback on configuration and use cases is welcome.

#### Using local Python modules

The `module_paths` profile setting lets you specify a list of filesystem paths containing additional Python modules. These paths are added to the dbt process's `sys.path`, which makes the modules importable within dbt. You can use this to include helper code in your project, such as custom `dbt-duckdb` plugins or shared libraries for Python models.

### Writing your own plugins

Defining your own dbt-duckdb plugin is as simple as creating a Python module that defines a class named `Plugin` that inherits from [dbt.adapters.duckdb.plugins.BasePlugin](dbt/adapters/duckdb/plugins/__init__.py). There are currently four methods that may be implemented in your `Plugin` class:

1. `initialize`: Takes in the `config` dictionary for the plugin defined in the profile and enables any additional configuration for the module based on the project. This method is called once when an instance of the `Plugin` class is created.
2. `configure_connection`: Takes an instance of the `DuckDBPyConnection` object used to connect to the DuckDB database and may perform any additional configuration of that object that is needed by the plugin, such as defining custom user-defined functions.
3. `load`: Takes a [SourceConfig](dbt/adapters/duckdb/utils.py) instance, which encapsulates the configuration for a dbt source and can optionally return a DataFrame-like object that DuckDB knows how to turn into a table.
4. `store`: Takes a [TargetConfig](dbt/adapters/duckdb/utils.py) instance, which encapsulates the configuration for an `external` materialization and can perform additional operations once the CSV, Parquet, or JSON file is written.

The [glue](dbt/adapters/duckdb/plugins/glue.py) and [sqlalchemy](dbt/adapters/duckdb/plugins/sqlalchemy.py) plugins are useful examples of using the `store` operation to register an AWS Glue database table or upload a DataFrame to an external database. `dbt-duckdb` also ships with a number of [built-in plugins](dbt/adapters/duckdb/plugins/) that can be used as examples for implementing your own.

### Roadmap

Things that we would like to add in the near future:

- Support for Delta and Iceberg external table formats, both as sources and destinations
- Make dbt incremental models and snapshots work with external materializations
