## dbt-duckdb

### Installation

This project is hosted on PyPI, so you should be able to install it and the necessary dependencies via:

`pip3 install dbt-duckdb`

The latest supported version targets `dbt-core` 1.2.x and `duckdb` version 0.5.0 (but should also work with 0.4.0 and 0.3.2!)

### Configuring your profile

[DuckDB](http://duckdb.org) is an embedded database, similar to SQLite, but designed for OLAP-style analytics instead of OLTP. The only
configuration parameter that is required in your profile (in addition to `type: duckdb`) is the `path` field, which should refer to
a path on your local filesystem where you would like the DuckDB database file (and it's associated write-ahead log) to be written.
You can also specify the `schema` parameter if you would like to use a schema besides the default (which is called `main`).

Prior to version 1.2.1, dbt-duckdb was limited to a single dbt execution thread; in version 1.2.1 this constraint was lifted and
now dbt-duckdb can run with as many threads as you can throw at it.

There is also a `database` field defined in the `DuckDBCredentials` class for consistency with the parent `Credentials` class,
but it defaults to `main` and setting it to be something else will likely cause strange things to happen that I cannot fully predict,
so, ya know, don't do that.

As of version 1.2.0, dbt-duckdb also allows you to configure your S3 settings in your credentials, including `s3_region` and
either `s3_session_token` or `s3_access_key_id` and `s3_secret_access_key`, so that you can use dbt-duckdb to read and transform
data stored in S3 files. You can also specify an arbitrary number of [DuckDB extensions](https://duckdb.org/docs/extensions/overview) to
load as part of your dbt-duckdb project using the `extensions: []` credentials field.

### Developer Workflow

If you need to add features to dbt-duckdb itself, use this workflow to get started:

```
$ git clone https://github.com/jwills/dbt-duckdb.git
$ cd dbt-duckdb
$ "hack on stuff for awhile"
$ pip3 install .
$ pytest tests/
```
