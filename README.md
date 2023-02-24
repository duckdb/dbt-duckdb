## dbt-duckdb

[DuckDB](http://duckdb.org) is an embedded database, similar to SQLite, but designed for OLAP-style analytics.
It is crazy fast and allows you to read and write data stored in CSV and Parquet files directly, without requiring you to load
them into the database first.

[dbt](http://getdbt.com) is the best way to manage a collection of data transformations written in SQL or Python for analytics
and data science. `dbt-duckdb` is the project that ties DuckDB and dbt together, allowing you to create a [Modern Data Stack In
A Box](https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html) or a simple and powerful data lakehouse- no Java or Scala
required.

### Installation

This project is hosted on PyPI, so you should be able to install it and the necessary dependencies via:

`pip3 install dbt-duckdb`

The latest supported version targets `dbt-core` 1.3.x and `duckdb` version 0.5.x, but we work hard to ensure that newer
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

The `database` property is special: prior to DuckDB `0.7.0` and dbt-duckdb `1.4.0`, it was always set to `main`,
because DuckDB did not have the concept of multiple databases accessible from a single connection.
As of dbt-duckdb `1.4.0` and DuckDB `0.7.0`, the value of the `database` property is automatically set to the basename of the
file in the `path` argument with the suffix removed (so for example if the `path` is `/tmp/a/dbfile.duckdb` the `database` argument will be
automatically set to `dbfile`). If you are running with the `path` equal to `:memory:`, then the name of the database will be `memory`.

### DuckDB Extensions and Settings

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

#### Fetching credentials from context
Instead of specifying the credentials through the settings block, you can also use the use_credential_provider property. If you set this to `aws` (currently the only supported implementation) and you have `boto3` installed in your python environment, we will fetch your AWS credentials using the credential provider chain as described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html). This means that you can use any supported mechanism from AWS to obtain credentials (e.g., web identity tokens).

### Attaching Additional Databases

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


### External Materializations and Sources

One of DuckDB's most powerful features is its ability to read and write CSV and Parquet files directly, without needing to import/export
them from the database first.

#### Writing to external files

In `dbt-duckdb`, creating models that are backed by external files is supported via the `external` materialization strategy:

```
{{ config(materialized='external', location='local/directory/file.parquet') }}
SELECT m.*, s.id IS NOT NULL as has_source_id
FROM {{ ref('upstream_model') }} m
LEFT JOIN {{ source('upstream', 'source') }} s USING (id)
```

| Option | Default | Description
| :---:    |  :---:    | ---
| location | `{{ name }}.{{ format }}` | The path to write/read the external materialization to/from. See below for more details.
| format | parquet | The format of the external file, either `parquet` or `csv`.
| delimiter | ,    | For CSV files, the delimiter to use for fields.
| glue_register | false | If true, try to register the file created by this model with the AWS Glue Catalog.
| glue_database | default | The name of the AWS Glue database to register the model with.

If no `location` argument is specified, then the external file will be named after the model.sql (or model.py) file that defined it with an extension that matches the file format (either `.parquet` or `.csv`). 

By default, external materializations are created in a local file relative to the current working directory, but you can change the default directory (or S3 bucket/prefix) by specifying the `external_root` setting in your DuckDB profile:

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
      external_root: "s3://my-bucket/my-prefix-path/"
  target: dev
```

#### Reading from external files

`dbt-duckdb` also includes support for referencing external CSV and Parquet files as dbt `source`s via the `external_location` meta option:

```
sources:
  - name: external_source
    meta:
      external_location: "s3://my-bucket/my-sources/{name}.parquet"
    tables:
      - name: source1
      - name: source2
```

Here, the `meta` options on `external_source` defines `external_location` as an [f-string](https://peps.python.org/pep-0498/) that allows us to express a pattern that indicates the location of any of the tables defined for that source. So a dbt model like:

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
options for the table itself and also be provided the necessary explicit function call, for example:

```
sources:
  - name: external_source
    meta:
      external_location: "s3://my-bucket/my-sources/{name}.csv"
    tables:
      - name: source1
      - name: source2
        meta:
          external_location: "read_csv('s3://my-bucket/my-sources/{name}.csv', delim='|', header=True, columns={'createdAt': 'DATE', 'id': 'VARCHAR', 'name': 'VARCHAR'})"
```

In this case, a dbt model like:

```
SELECT * 
FROM {{ source('external_source', 'source2') }}
```

will be compiled as:

```
SELECT *
FROM read_csv('s3://my-bucket/my-sources/source2.csv', delim='|', header=True, columns={'createdAt': 'DATE', 'id': 'VARCHAR', 'name': 'VARCHAR'})
```

#### Re-running external models with an in-memory version of dbt-duckdb
When using `:memory:` as the DuckDB database, subsequent dbt runs can fail when selecting a subset of models that depend on external tables. This is because external Parquet or CSV files are only registered as DuckDB views when they are created, not when they are referenced. To overcome this issue we have provided the `register_upstream_external_models` macro that can be triggered at the beginning of a run. To enable this automatic registration, place the following in your `dbt_project.yml` file:

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
Pandas DataFrame or Arrow table, and the return value of the `def models` function can be either a DuckDB `Relation`, a Pandas DataFrame,
or an Arrow Table.

### Roadmap

Things that we would like to add in the near future:

* Support for Delta and Iceberg external table formats (both as sources and destinations)
* Make dbt's incremental models and snapshots work with external materializations
* Make AWS Glue registration a first-class concept and add support for Snowflake/BigQuery registrations
