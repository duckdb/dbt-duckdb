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

As of version 1.2.3, you can load any supported [DuckDB extensions](https://duckdb.org/docs/extensions/overview) by listing them in
the `extensions` field in your profile. You can also set any additional [DuckDB configuration options](https://duckdb.org/docs/sql/configuration)
via the `settings` field, including options that are supported in any loaded extensions. For example, to be able to connect to S3 and read/write
Parquet files using an AWS access key and secret, your profile would look something like this:

```
default:
  outputs:
    dev:
      path: /tmp/dbt_test.db
      schema: analytics
      type: duckdb
      threads: 4
      extensions:
        - httpfs
        - parquet
      settings:
        s3_region: my-aws-region
        s3_access_key_id: "{{ env_var('S3_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
  target: dev
```


### Developer Workflow

If you need to add features to dbt-duckdb itself, use this workflow to get started:

```
$ git clone https://github.com/jwills/dbt-duckdb.git
$ cd dbt-duckdb
$ "hack on stuff for awhile"
$ pip3 install .
$ pytest tests/
```
