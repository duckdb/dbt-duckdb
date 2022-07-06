## dbt-duckdb

### Installation

This project is hosted on PyPI, so you should be able to install it and the necessary dependencies via:

`pip3 install dbt-duckdb`

The latest supported version targets `dbt-core` 1.1.x and `duckdb` 0.3.2.

### Configuring your profile

[DuckDB](http://duckdb.org) is an embedded database, similar to SQLite, but designed for OLAP-style analytics instead of OLTP. The only
configuration parameter that is required in your profile (in addition to `type: duckdb`) is the `path` field, which should refer to
a path on your local filesystem where you would like the DuckDB database file (and it's associated write-ahead log) to be written.
You can also specify the `schema` parameter if you would like to use a schema besides the default (which is called `main`).

Note that dbt-duckdb currently only works in single-threaded mode, just like [dbt-sqlite](https://github.com/codeforkjeff/dbt-sqlite); if
you try to run dbt-duckdb with multiple `threads` configured in your profile, the adapter will raise an exception. We are
looking forward to fixing this limitation in the near future.

There is also a `database` field defined in the `DuckDBCredentials` class for consistency with the parent `Credentials` class,
but it defaults to `main` and setting it to be something else will likely cause strange things to happen that I cannot fully predict,
so, ya know, don't do that.

### Developer Workflow
If you find that you need to add a feature to DuckDB in order to implement some functionality in dbt-duckdb, here is
the workflow you can use for doing local development. First, you need to clone and build DuckDB from source:

```
$ git clone https://github.com/duckdb/duckdb.git
$ cd duckdb
$ pip3 install -e tools/pythonpkg
```

in order to do a local install off of the main branch, and then run:

```
$ git clone https://github.com/jwills/dbt-duckdb.git
$ cd dbt-duckdb
$ pip3 install .
```

to install `dbt-duckdb`, at which point you will be able to run:

```
$ pip3 install pytest-dbt-adapter
$ pytest test/duckdb.dbtspec
```

to exercise the [dbt-adapter-tests](https://github.com/fishtown-analytics/dbt-adapter-tests) locally against your build of DuckDB
(note that you also need to update `setup.py` in this directory to ensure that you are using your local version of duckdb, and not
the released duckdb version 0.3.2)
