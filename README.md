## dbt-duckdb

### Installation
This project is under active development and currently requires a local build of the development version of [DuckDB](https://github.com/cwida/duckdb)
in order to test it out. If you would like to try it, you can run:

```
$ git clone https://github.com/cwida/duckdb.git
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

to exercise the [dbt-adapter-tests](https://github.com/fishtown-analytics/dbt-adapter-tests) locally against DuckDB.

### Configuring your profile

[DuckDB](http://duckdb) is an embedded database, similar to SQLite, but designed for OLAP-style analytics instead of OLTP. The only
configuration parameter that is required in your profile (in addition to `type: duckdb`) is the `path` field, which should refer to
a path on your local filesystem where you would like the DuckDB database file (and it's associated write-ahead log) to be written.
You can also specify the `schema` parameter if you would like to use a schema besides the default (which is called `main`).

There is also a `database` field defined in the `DuckDBCredentials` class for consistency with the parent `Credentials` class,
but it defaults to `main` and setting it to be something else will likely cause strange things to happen that I cannot fully predict,
so, ya know, don't do that.
