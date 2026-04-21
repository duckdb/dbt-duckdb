## dbt-duckdb

[DuckDB](http://duckdb.org) is an embedded database, similar to SQLite, but designed for OLAP-style analytics.
It is crazy fast and allows you to read and write data stored in CSV, JSON, and Parquet files directly, without requiring you to load
them into the database first.

[dbt](http://getdbt.com) is the best way to manage a collection of data transformations written in SQL or Python for analytics
and data science. `dbt-duckdb` is the project that ties DuckDB and dbt together, allowing you to create a [Modern Data Stack In
A Box](https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html) or a simple and powerful data lakehouse with Python.

### Installation

This project is hosted on PyPI, so you should be able to install it and the necessary dependencies via:

`pip3 install dbt-duckdb`

The latest supported version targets `dbt-core` versions >= 1.8.x and `duckdb` version 1.1.x, but we work hard to ensure that newer
versions of DuckDB will continue to work with the adapter as they are released.

### Documentation

Most user-facing setup and configuration documentation for `dbt-duckdb` now lives on docs.getdbt.com:

- [connecting to DuckDB with dbt](https://docs.getdbt.com/docs/local/connect-data-platform/duckdb-setup)
- [configuring DuckDB with dbt](https://docs.getdbt.com/reference/resource-configs/duckdb-configs)

Use this README for repository-specific topics that are better kept close to the adapter source.

#### Configuring dbt-duckdb Plugins

dbt-duckdb has its own [plugin](dbt/adapters/duckdb/plugins/__init__.py) system to enable advanced users to extend
dbt-duckdb with additional functionality, including:

* Defining [custom Python UDFs](https://duckdb.org/docs/api/python/function.html) on the DuckDB database connection
so that they can be used in your SQL models
* Loading source data from [Excel](dbt/adapters/duckdb/plugins/excel.py), [Google Sheets](dbt/adapters/duckdb/plugins/gsheet.py), or [SQLAlchemy](dbt/adapters/duckdb/plugins/sqlalchemy.py) tables

You can find more details on [how to write your own plugins here](#writing-your-own-plugins). To configure a plugin for use
in your dbt project, use the `plugins` property on the profile:

```
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
```

Every plugin must have a `module` property that indicates where the `Plugin` class to load is defined. There is
a set of built-in plugins that are defined in [dbt.adapters.duckdb.plugins](dbt/adapters/duckdb/plugins/) that
may be referenced by their base filename (e.g., `excel` or `gsheet`), while user-defined plugins (which are
described later in this document) should be referred to via their full module path name (e.g. a `lib.my.custom` module that defines a class named `Plugin`.)

Each plugin instance has a name for logging and reference purposes that defaults to the name of the module
but that may be overridden by the user by setting the `alias` property in the configuration. Finally,
modules may be initialized using an arbitrary set of key-value pairs that are defined in the
`config` dictionary. In this example, we initialize the `gsheet` plugin with the setting `method: oauth` and we
initialize the `sqlalchemy` plugin (aliased as "sql") with a `connection_url` that is set via an environment variable.

Please remember that using plugins may require you to add additional dependencies to the Python environment that your dbt-duckdb pipeline runs in:

* `excel` depends on `pandas`, and `openpyxl` or `xlsxwriter` to perform writes
* `gsheet` depends on `gspread` and `pandas`
* `iceberg` depends on `pyiceberg` and Python >= 3.10
* `sqlalchemy` depends on `pandas`, `sqlalchemy`, and the driver(s) you need

**Experimental:**

* `delta` depends on `deltalake`, [an example project](https://github.com/milicevica23/dbt-duckdb-delta-plugin-demo)

**Note:** Be aware that experimental features can change over time, and we would like your feedback on config and possible different use cases.

#### Using Local Python Modules

In dbt-duckdb 1.6.0, we added a new profile setting named `module_paths` that allows users to specify a list
of paths on the filesystem that contain additional Python modules that should be added to the Python processes'
`sys.path` property. This allows users to include additional helper Python modules in their dbt projects that
can be accessed by the running dbt process and used to define custom dbt-duckdb Plugins or library code that is
helpful for creating dbt Python models.

### Writing Your Own Plugins

Defining your own dbt-duckdb plugin is as simple as creating a python module that defines a class named `Plugin` that
inherits from [dbt.adapters.duckdb.plugins.BasePlugin](dbt/adapters/duckdb/plugins/__init__.py). There are currently
four methods that may be implemented in your Plugin class:

1. `initialize`: Takes in the `config` dictionary for the plugin that is defined in the profile to enable any
additional configuration for the module based on the project; this method is called once when an instance of the
`Plugin` class is created.
1. `configure_connection`: Takes an instance of the `DuckDBPyConnection` object used to connect to the DuckDB
database and may perform any additional configuration of that object that is needed by the plugin, like defining
custom user-defined functions.
1. `load`: Takes a [SourceConfig](dbt/adapters/duckdb/utils.py) instance, which encapsulates the configuration for a
a dbt source and can optionally return a DataFrame-like object that DuckDB knows how to turn into a table (this is
similar to a dbt-duckdb Python model, but without the ability to `ref` any models or access any information beyond
the source config.)
1. `store`: Takes a [TargetConfig](dbt/adapters/duckdb/utils.py) instance, which encapsulates the configuration for
an `external` materialization and can perform additional operations once the CSV/Parquet/JSON file is written. The
[glue](dbt/adapters/duckdb/plugins/glue.py) and [sqlalchemy](dbt/adapters/duckdb/plugins/sqlalchemy.py) are examples
that demonstrate how to use the `store` operation to register an AWS Glue database table or upload a DataFrame to
an external database, respectively.

dbt-duckdb ships with a number of [built-in plugins](dbt/adapters/duckdb/plugins/) that can be used as examples
for implementing your own.

### Roadmap

Things that we would like to add in the near future:

* Support for Delta and Iceberg external table formats (both as sources and destinations)
* Make dbt's incremental models and snapshots work with external materializations
