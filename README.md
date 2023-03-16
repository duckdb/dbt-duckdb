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

The latest supported version targets `dbt-core` 1.4.x and `duckdb` version 0.7.x, but we work hard to ensure that newer
versions of DuckDB will continue to work with the adapter as they are released. If you would like to use our new (and experimental!)
support for persisting the tables that DuckDB creates to the [AWS Glue Catalog](https://aws.amazon.com/glue/), you should install
`dbt-duckdb[glue]` in order to get the AWS dependencies as well.

### Configuring Your Profile

A minimal dbt-duckdb profile only needs two settings, `type` and `path`:

````
default:
  outputs:
   dev:
     type: duckdb
     path: /tmp/dbt.duckdb
  target: dev
````

The `path` field should normally be the path to a local DuckDB file on your filesystem, but it can also be set equal to `:memory:` if you
would like to run an in-memory only version of dbt-duckdb. Keep in mind that if you are using the in-memory mode,
any models that you want to keep from the dbt run will need to be persisted using one of the external materialization strategies described below.

`dbt-duckdb` also supports common profile fields like `schema` and `threads`, but the `database` property is special: it's value is automatically set
to the basename of the file in the `path` argument with the suffix removed. For example, if the `path` is `/tmp/a/dbfile.duckdb`, the `database`
field will be set to `dbfile`. If you are running with the `path` equal to `:memory:`, then the name of the database will be `memory`.

#### DuckDB Extensions, Settings, and Filesystems

You can load any supported [DuckDB extensions](https://duckdb.org/docs/extensions/overview) by listing them in
the `extensions` field in your profile. You can also set any additional [DuckDB configuration options](https://duckdb.org/docs/sql/configuration)
via the `settings` field, including options that are supported in any loaded extensions. For example, to be able to connect to S3 and read/write
Parquet files using an AWS access key and secret, your profile would look something like this:

```
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - httpfs
        - parquet
      settings:
        s3_region: my-aws-region
        s3_access_key_id: "{{ env_var('S3_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
  target: dev
```

As of verion `1.4.1`, we have added (experimental!) support for DuckDB's (experimental!) support for filesystems
implemented via [fsspec](https://duckdb.org/docs/guides/python/filesystems.html). The `fsspec` library provides
support for reading and writing files from a [variety of cloud data storage systems](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations)
including S3, GCS, and Azure Blob Storage. You can configure a list of fsspec-compatible implementations for use with your dbt-duckdb project by installing the relevant Python modules
and configuring your profile like so:

```
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      filesystems:
        - fs: s3
          anon: false
          key: "{{ env_var('S3_ACCESS_KEY_ID') }}"
          secret: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
          client_kwargs:
            endpoint_url: "http://localhost:4566"
  target: dev
```

Here, the `filesystems` property takes a list of configurations, where each entry must have a property named `fs` that indicates which `fsspec` protocol
to load (so `s3`, `gcs`, `abfs`, etc.) and then an arbitrary set of other key-value pairs that are used to configure the `fsspec` implementation. You can see a simple example project that
illustrates the usage of this feature to connect to a Localstack instance running S3 from dbt-duckdb [here](https://github.com/jwills/s3-demo).

#### Fetching credentials from context
Instead of specifying the credentials through the settings block, you can also use the use_credential_provider property. If you set this to `aws` (currently the only supported implementation) and you have `boto3` installed in your python environment, we will fetch your AWS credentials using the credential provider chain as described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html). This means that you can use any supported mechanism from AWS to obtain credentials (e.g., web identity tokens).

#### Attaching Additional Databases

DuckDB version `0.7.0` and dbt-duckdb version `1.4.0` support [attaching additional databases](https://duckdb.org/docs/sql/statements/attach.html) to your dbt-duckdb run so that you can read
and write from multiple databases. Additional databases may be configured using [dbt run hooks](https://docs.getdbt.com/docs/build/hooks-operations) or via the `attach` argument
in your profile:

```
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      attach:
        - path: /tmp/other.duckdb
        - path: ./yet/another.duckdb
          alias: yet_another
        - path: s3://yep/even/this/works.duckdb
          read_only: true
        - path: sqlite.db
          type: sqlite
```

The attached databases may be referred to in your dbt sources and models by either the basename of the database file minus its suffix (e.g., `/tmp/other.duckdb` is the `other` database
and `s3://yep/even/this/works.duckdb` is the `works` database) or by an alias that you specify (so the `./yet/another.duckdb` database in the above configuration is referred to
as `yet_another` instead of `another`.) Note that these additional databases do not necessarily have to be DuckDB files: DuckDB's storage and catalog engines are pluggable, and
DuckDB `0.7.0` ships with support for reading and writing from attached SQLite databases. You can indicate the type of the database you are connecting to via the `type` argument,
which currently supports `duckdb` and `sqlite`.


### Reading and Writing External Files

One of DuckDB's most powerful features is its ability to read and write CSV, JSON, and Parquet files directly, without needing to import/export
them from the database first.

#### Reading from external files

You may reference external files in your dbt model's either directly or as dbt `source`s by configuring the `external_location`
meta option on the source:

```
sources:
  - name: external_source
    meta:
      external_location: "s3://my-bucket/my-sources/{name}.parquet"
    tables:
      - name: source1
      - name: source2
```

Here, the `meta` options on `external_source` defines `external_location` as an [f-string](https://peps.python.org/pep-0498/) that
allows us to express a pattern that indicates the location of any of the tables defined for that source. So a dbt model like:

```
SELECT *
FROM {{ source('external_source', 'source1') }}
```

will be compiled as:

```
SELECT *
FROM 's3://my-bucket/my-sources/source1.parquet'
```

If one of the source tables deviates from the pattern or needs some other special handling, then the `external_location` can also be set on the `meta`
options for the table itself, for example:

```
sources:
  - name: external_source
    meta:
      external_location: "s3://my-bucket/my-sources/{name}.parquet"
    tables:
      - name: source1
      - name: source2
        meta:
          external_location: "read_parquet(['s3://my-bucket/my-sources/source2a.parquet', 's3://my-bucket/my-sources/source2b.parquet'])"
```

In this situation, the `external_location` setting on the `source2` table will take precedence, so a dbt model like:

```
SELECT *
FROM {{ source('external_source', 'source2') }}
```

will be compiled to the SQL query:

```
SELECT *
FROM read_parquet(['s3://my-bucket/my-sources/source2a.parquet', 's3://my-bucket/my-sources/source2b.parquet'])
```

Note that the value of the `external_location` property does not need to be a path-like string; it can also be a function
call, which is helpful in the case that you have an external source that is a CSV file which requires special handling for DuckDB
to load it correctly:

```
sources:
  - name: flights_source
    tables:
      - name: flights
        meta:
          external_location: "read_csv('flights.csv', types={'FlightDate': 'DATE'}, names=['FlightDate', 'UniqueCarrier'])"
```

#### Writing to external files

We support creating dbt models that are backed by external files via the `external` materialization strategy:

```
{{ config(materialized='external', location='local/directory/file.parquet') }}
SELECT m.*, s.id IS NOT NULL as has_source_id
FROM {{ ref('upstream_model') }} m
LEFT JOIN {{ source('upstream', 'source') }} s USING (id)
```

| Option | Default | Description
| :---:    |  :---:    | ---
| location | `{{ name }}.{{ format }}` | The path to write the external materialization to. See below for more details.
| format | parquet | The format of the external file (parquet, csv, or json)
| delimiter | ,    | For CSV files, the delimiter to use for fields.
| options | None | Any other options to pass to DuckDB's `COPY` operation (e.g., `partition_by`, `codec`, etc.)
| glue_register | false | If true, try to register the file created by this model with the AWS Glue Catalog.
| glue_database | default | The name of the AWS Glue database to register the model with.

If the `location` argument is specified, it must be a filename (or S3 bucket/path), and dbt-duckdb will attempt to infer
the `format` argument from the file extension of the `location` if the `format` argument is unspecified (this functionality was
added in version 1.4.1.)

If the `location` argument is _not_ specified, then the external file will be named after the model.sql (or model.py) file that defined it
with an extension that matches the `format` argument (`parquet`, `csv`, or `json`). By default, the external files are created
relative to the current working directory, but you can change the default directory (or S3 bucket/prefix) by specifying the
`external_root` setting in your DuckDB profile.

#### Re-running external models with an in-memory version of dbt-duckdb
When using `:memory:` as the DuckDB database, subsequent dbt runs can fail when selecting a subset of models that depend on external tables. This is because external files are only registered as  DuckDB views when they are created, not when they are referenced. To overcome this issue we have provided the `register_upstream_external_models` macro that can be triggered at the beginning of a run. To enable this automatic registration, place the following in your `dbt_project.yml` file:

```yaml
on-run-start:
  - "{{ register_upstream_external_models() }}"
```

### Python Support

dbt added support for [Python models in version 1.3.0](https://docs.getdbt.com/docs/build/python-models). For most data platforms,
dbt will package up the Python code defined in a `.py` file and ship it off to be executed in whatever Python environment that
data platform supports. However, in `dbt-duckdb`, the local machine *is* the data platform, and so we support executing any Python
code that will run on your machine via an [exec](https://realpython.com/python-exec/) call. The value of the `dbt.ref` and `dbt.source`
functions will be a [DuckDB Relation](https://duckdb.org/docs/api/python/reference/) object that can be easily converted into a
Pandas DataFrame or Arrow table, and the return value of the `def models` function can be _any_ Python object that DuckDB knows how
to turn into a relation, including a Pandas or Polars `DataFrame`, a DuckDB `Relation`, or an Arrow `Table`, `Dataset`, `RecordBatchReader`, or
`Scanner`.

### Roadmap

Things that we would like to add in the near future:

* Support for Delta and Iceberg external table formats (both as sources and destinations)
* Make dbt's incremental models and snapshots work with external materializations
* Make AWS Glue registration a first-class concept and add support for Snowflake/BigQuery registrations
