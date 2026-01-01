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

### Configuring Your Profile

A super-minimal dbt-duckdb profile only needs *one* setting:

````
default:
  outputs:
    dev:
      type: duckdb
  target: dev
````

This will run your dbt-duckdb pipeline against an in-memory DuckDB database that will not be persisted after your run completes. This may
not seem very useful at first, but it turns out to be a powerful tool for a) testing out data pipelines, either locally or in CI jobs and
b) running data pipelines that operate purely on external CSV, Parquet, or JSON files. More details on how to work with external data files
in dbt-duckdb are provided in the docs on [reading and writing external files](#reading-and-writing-external-files).

To have your dbt pipeline persist relations in a DuckDB file, set the `path` field in your profile to the path
of the DuckDB file that you would like to read and write on your local filesystem. (For in-memory pipelines, the `path`
is automatically set to the special value `:memory:`). By default, the `path` is relative to your `profiles.yml` file location.
If the database doesn't exist at the specified `path`, DuckDB will automatically create it.

`dbt-duckdb` also supports common profile fields like `schema` and `threads`, but the `database` property is special: its value is automatically set
to the basename of the file in the `path` argument with the suffix removed. For example, if the `path` is `/tmp/a/dbfile.duckdb`, the `database`
field will be set to `dbfile`. If you are running in in-memory mode, then the `database` property will be automatically set to `memory`.

#### Using MotherDuck

As of `dbt-duckdb` 1.5.2, you can connect to a DuckDB instance running on [MotherDuck](http://www.motherduck.com) by setting your `path` to use a [md:<database> connection string](https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication), just as you would with the DuckDB CLI
or the Python API.

MotherDuck databases generally work the same way as local DuckDB databases from the perspective of dbt, but
there are a [few differences to be aware of](https://motherduck.com/docs/architecture-and-capabilities#considerations-and-limitations):
1. MotherDuck is compatible with client DuckDB versions 0.10.2 and older.
1. MotherDuck preloads a set of the most common DuckDB extensions for you, but does not support loading custom extensions or user-defined functions.

As of `dbt-duckdb` 1.9.6, you can also connect to a DuckDB instance running [hosted DuckLake on MotherDuck](https://motherduck.com/blog/ducklake-motherduck/) by creating a DuckLake on MotherDuck and then setting `is_ducklake: true` in your `profiles.yml`.

```sql
-- to use create your own database in MotherDuck first
CREATE DATABASE my_ducklake
  (TYPE ducklake, DATA_PATH 's3://...')
```

An example profile is show below under "Attaching Additional Databases". DuckLake must be identified so that safe DDL operations are applied by dbt.

#### DuckDB Extensions, Settings, and Filesystems

You can install and load any core [DuckDB extensions](https://duckdb.org/docs/extensions/overview) by listing them in
the `extensions` field in your profile as a string. You can also set any additional [DuckDB configuration options](https://duckdb.org/docs/sql/configuration)
via the `settings` field, including options that are supported in the loaded extensions. You can also configure extensions from outside of the core
extension repository (e.g., a community extension) by configuring the extension as a `name`/`repo` pair:

```
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - httpfs
        - parquet
        - name: h3
          repo: community
        - name: uc_catalog
          repo: core_nightly
  target: dev
```

To use the [DuckDB Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html), you can use the `secrets` field. For example, to be able to connect to S3 and read/write
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
      secrets:
        - type: s3
          region: my-aws-region
          key_id: "{{ env_var('S3_ACCESS_KEY_ID') }}"
          secret: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
  target: dev
```

As of version `1.4.1`, we have added (experimental!) support for DuckDB's (experimental!) support for filesystems
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

Instead of specifying the credentials through the settings block, you can also use the `CREDENTIAL_CHAIN` secret provider. This means that you can use any supported mechanism from AWS to obtain credentials (e.g., web identity tokens). You can read more about the secret providers [here](https://duckdb.org/docs/configuration/secrets_manager.html#secret-providers). To use the `CREDENTIAL_CHAIN` provider and automatically fetch credentials from AWS, specify the `provider` in the `secrets` key:

```
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - httpfs
        - parquet
      secrets:
        - type: s3
          provider: credential_chain
  target: dev
```

#### Scoped credentials by storage prefix

Secrets can be scoped, such that different storage path can use different credentials.

```
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - httpfs
        - parquet
      secrets:
        - type: s3
          provider: credential_chain
          scope: [ "s3://bucket-in-eu-region", "s3://bucket-2-in-eu-region" ]
          region: "eu-central-1"
        - type: s3
          region: us-west-2
          scope: "s3://bucket-in-us-region"
```

When fetching a secret for a path, the secret scopes are compared to the path, returning the matching secret for the path. In the case of multiple matching secrets, the longest prefix is chosen.

#### Attaching Additional Databases

DuckDB supports [attaching additional databases](https://duckdb.org/docs/sql/statements/attach.html) to your dbt-duckdb run so that you can read
and write from multiple databases. Additional databases may be configured via the `attach` argument
in your profile that was added in dbt-duckdb `1.4.0`:

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
        - path: postgresql://username@hostname/dbname
          type: postgres
        # Using the options dict for arbitrary ATTACH options
        - path: /tmp/special.duckdb
          options:
            cache_size: 1GB
            threads: 4
            enable_fsst: true
```

For DuckLake, use `ducklake:` for local; for MotherDuck-managed DuckLake use `md:` with `is_ducklake: true`.

```yaml
attach:
  - path: "ducklake:my_ducklake.ddb"
  - path: "md:my_other_ducklake"
    is_ducklake: true
```


The attached databases may be referred to in your dbt sources and models by either the basename of the database file minus its suffix (e.g., `/tmp/other.duckdb` is the `other` database
and `s3://yep/even/this/works.duckdb` is the `works` database) or by an alias that you specify (so the `./yet/another.duckdb` database in the above configuration is referred to
as `yet_another` instead of `another`.) Note that these additional databases do not necessarily have to be DuckDB files: DuckDB's storage and catalog engines are pluggable, and
DuckDB ships with support for reading and writing from attached databases. You can indicate the type of the database you are connecting to via the `type` argument,
which currently supports `duckdb`, `sqlite` and `postgres`.

##### Arbitrary ATTACH Options

As DuckDB continues to add new attachment options, you can use the `options` dictionary to specify any additional key-value pairs that will be passed to the `ATTACH` statement. This allows you to take advantage of new DuckDB features without waiting for explicit support in dbt-duckdb:

```
attach:
  # Standard way using direct fields
  - path: /tmp/db1.duckdb
    type: sqlite
    read_only: true

  # New way using options dict (equivalent to above)
  - path: /tmp/db2.duckdb
    options:
      type: sqlite
      read_only: true

  # Mix of both (no conflicts allowed)
  - path: /tmp/db3.duckdb
    type: sqlite
    options:
      block_size: 16384

  # Using options dict for future DuckDB attachment options
  - path: /tmp/db4.duckdb
    options:
      type: duckdb
      # Example: hypothetical future options DuckDB might add
      compression: lz4
      memory_limit: 2GB
```

Note: If you specify the same option in both a direct field (`type`, `secret`, `read_only`) and in the `options` dict, dbt-duckdb will raise an error to prevent conflicts.

#### AWS S3 Tables (Iceberg) Support

As of dbt-duckdb 1.10.1, you can work with [AWS S3 Tables](https://aws.amazon.com/s3/features/tables/) which use Apache Iceberg format. S3 Tables provide managed Iceberg tables with automatic schema evolution, ACID transactions, and optimized query performance.

##### Configuring S3 Tables

To use S3 Tables, attach them using the `iceberg` type and specify `endpoint_type: s3_tables`:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - iceberg
      attach:
        - path: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
          alias: "s3_tables"
          type: "iceberg"
          endpoint_type: "s3_tables"
          read_only: false
  target: dev
```

**Important**: You must install PyIceberg for schema evolution support:
```bash
pip install dbt-duckdb[s3tables]
# or
pip install pyiceberg>=0.5.0
```

##### S3 Tables Features

**Automatic Schema Evolution**: When your dbt models add or remove columns, dbt-duckdb automatically evolves the Iceberg schema using PyIceberg. Columns removed from your model are preserved in the table with NULL values (no data loss).

**Supported Materializations**:
- `table`: Full refresh using DROP + CREATE pattern
- `incremental`: DELETE + INSERT pattern with automatic schema evolution

**Iceberg Table Properties**: You can specify Iceberg table properties when creating tables:

```sql
{{
  config(
    materialized='table',
    database='s3_tables',
    iceberg_properties={
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'snappy'
    }
  )
}}

SELECT * FROM source_table
```

##### Incremental Models with Schema Evolution

For incremental models, dbt-duckdb automatically handles schema changes:

```sql
{{
  config(
    materialized='incremental',
    database='s3_tables',
    unique_key='customer_id',
    watermark_column='updated_at'  -- Optional: for deduplication
  )
}}

SELECT 
  customer_id,
  name,
  email,
  new_column  -- Automatically added to target table
FROM source_table
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
```

**Schema Evolution Behavior**:
- **New columns**: Automatically added to target table via PyIceberg
- **Removed columns**: Preserved in target table, receive NULL values for new rows
- **No data loss**: Historical data with removed columns remains intact
- **No breaking changes**: Downstream systems can still query removed columns

##### S3 Tables Limitations

Due to Iceberg/S3 Tables constraints, the following are NOT supported:
- `CREATE OR REPLACE TABLE` (use DROP + CREATE instead)
- `ALTER TABLE` via SQL (use PyIceberg for schema changes)
- `UPDATE` or `MERGE INTO` statements
- `DROP TABLE CASCADE`

dbt-duckdb automatically handles these limitations by using appropriate patterns (DROP + CREATE for full refresh, DELETE + INSERT for incremental).

##### Authentication

S3 Tables use AWS credentials from your environment:
- IAM role (recommended for EC2/ECS/Lambda)
- AWS CLI configuration (`~/.aws/credentials`)
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

Ensure your IAM role/user has permissions for:
- `s3tables:GetTable`
- `s3tables:PutTable`
- `s3tables:DeleteTable`
- `s3tables:GetTableMetadata`
- `s3tables:PutTableMetadata`

##### Example Project Structure

```
my_dbt_project/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── staging/
    │   └── stg_customers.sql  -- incremental with schema evolution
    └── marts/
        └── dim_customers.sql  -- table materialization
```

**profiles.yml**:
```yaml
my_project:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - iceberg
      attach:
        - path: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-iceberg-bucket"
          alias: "s3_tables"
          type: "iceberg"
          endpoint_type: "s3_tables"
          read_only: false
  target: dev
```

**models/staging/stg_customers.sql**:
```sql
{{
  config(
    materialized='incremental',
    database='s3_tables',
    unique_key='customer_id',
    watermark_column='updated_at'
  )
}}

SELECT 
  customer_id,
  name,
  email,
  updated_at
FROM raw.customers
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

##### S3 Tables Configuration Reference

The following table lists all configuration parameters available for S3 Tables (Iceberg) models:

| Parameter | Type | Applies To | Required | Default | Description | Example |
|-----------|------|------------|----------|---------|-------------|---------|
| `database` | string | Both | **Yes** | - | The alias of the attached S3 Tables catalog | `database='s3_tables'` |
| `materialized` | string | Both | **Yes** | - | Materialization strategy: `table` or `incremental` | `materialized='incremental'` |
| `unique_key` | string/list | Incremental | **Yes** (incremental) | - | Column(s) used to identify records for DELETE + INSERT operations | `unique_key='customer_id'` or `unique_key=['id', 'date']` |
| `watermark_column` | string | Incremental | No | - | Column for deduplication (keeps latest record per unique_key based on this column) | `watermark_column='updated_at'` |
| `precombine_column` | string | Incremental | No | - | Alias for `watermark_column` (same functionality) | `precombine_column='modified_at'` |
| `partition_by` | string/list | Both | No | - | Partition specification(s). Supports: identity (`'country'`), day (`'day(order_date)'`), month (`'month(order_date)'`), year (`'year(order_date)'`), hour (`'hour(timestamp_col)'`). **Note:** bucket and truncate transforms are not supported by AWS S3 Tables API | `partition_by='country'` or `partition_by=['year(order_date)', 'country']` |
| `iceberg_properties` | dict | Both | No | `{}` | Iceberg table properties to set (e.g., compression, format settings) | `iceberg_properties={'write.format.default': 'parquet', 'write.parquet.compression-codec': 'snappy'}` |
| `use_pyiceberg_writes` | boolean | Incremental | No | `false` | Force use of PyIceberg for DELETE + INSERT (automatically enabled when `partition_by` is set) | `use_pyiceberg_writes=true` |
| `on_schema_change` | string | Incremental | No | `'ignore'` | Not applicable to S3 Tables (schema evolution is automatic) | - |

**Common Parameters (Both Table and Incremental):**
- `database`: Must reference an attached S3 Tables catalog
- `partition_by`: Defines how data is partitioned in S3
- `iceberg_properties`: Sets Iceberg-specific table properties

**Table Materialization Only:**
- Uses DROP + CREATE pattern for full refresh
- All configuration applied during table creation

**Incremental Materialization Only:**
- `unique_key`: Required for identifying which rows to update
- `watermark_column`/`precombine_column`: Optional deduplication based on timestamp or version column
- `use_pyiceberg_writes`: Automatically enabled for partitioned tables (DuckDB limitation)

**Configuration Examples:**

**Example 1: Simple Table with Partitioning**
```sql
{{
  config(
    materialized='table',
    database='s3_tables',
    partition_by='country'
  )
}}

SELECT 
  customer_id,
  name,
  country,
  created_at
FROM source_table
```

**Example 2: Incremental with Watermark and Multiple Partitions**
```sql
{{
  config(
    materialized='incremental',
    database='s3_tables',
    unique_key='customer_id',
    watermark_column='updated_at',
    partition_by=['year(order_date)', 'country']
  )
}}

SELECT 
  customer_id,
  order_date,
  country,
  amount,
  updated_at
FROM orders
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

**Example 3: Table with Iceberg Properties**
```sql
{{
  config(
    materialized='table',
    database='s3_tables',
    partition_by='day(event_timestamp)',
    iceberg_properties={
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'snappy',
      'write.metadata.compression-codec': 'gzip'
    }
  )
}}

SELECT * FROM events
```

**Example 4: Incremental with Composite Unique Key**
```sql
{{
  config(
    materialized='incremental',
    database='s3_tables',
    unique_key=['user_id', 'event_date'],
    watermark_column='event_timestamp',
    partition_by='month(event_date)'
  )
}}

SELECT 
  user_id,
  event_date,
  event_type,
  event_timestamp
FROM user_events
{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

**Partition Transform Reference:**

| Transform | Syntax | Description | Example |
|-----------|--------|-------------|---------|
| Identity | `'column_name'` | Partition by exact column value | `partition_by='country'` |
| Day | `'day(date_column)'` | Partition by day (YYYY-MM-DD) | `partition_by='day(order_date)'` |
| Month | `'month(date_column)'` | Partition by month (YYYY-MM) | `partition_by='month(order_date)'` |
| Year | `'year(date_column)'` | Partition by year (YYYY) | `partition_by='year(order_date)'` |
| Hour | `'hour(timestamp_column)'` | Partition by hour (YYYY-MM-DD-HH) | `partition_by='hour(event_timestamp)'` |

**Note:** Multiple partitions can be specified as a list: `partition_by=['year(order_date)', 'country', 'region']`

For more details on S3 Tables, see the [AWS S3 Tables documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html).

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

### Reading and Writing External Files

One of DuckDB's most powerful features is its ability to read and write CSV, JSON, and Parquet files directly, without needing to import/export
them from the database first.

#### Reading from external files

You may reference external files in your dbt models either directly or as dbt `source`s by configuring the `external_location`
in either the `meta` or the `config` option on the source definition. The difference is that settings under the `meta` option
will be propagated to the documentation for the source generated via `dbt docs generate`, but the settings under the `config`
option will not be. Any source settings that should be excluded from the docs should be specified via `config`, while any
options that you would like to be included in the generated documentation should live under `meta`.

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
        config:
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
call, which is helpful in the case that you have an external source that is a CSV file which requires special handling for DuckDB to load it correctly:

```
sources:
  - name: flights_source
    tables:
      - name: flights
        config:
          external_location: "read_csv('flights.csv', types={'FlightDate': 'DATE'}, names=['FlightDate', 'UniqueCarrier'])"
          formatter: oldstyle
```

Note that we need to override the default `str.format` string formatting strategy for this example
because the `types={'FlightDate': 'DATE'}` argument to the `read_csv` function will be interpreted by
`str.format` as a template to be matched on, which will cause a `KeyError: "'FlightDate'"` when we attempt
to parse the source in a dbt model. The `formatter` configuration option for the source indicates whether
we should use `newstyle` string formatting (the default), `oldstyle` string formatting, or `template` string
formatting. You can read up on the strategies the various string formatting techniques use at this
[Stack Overflow answer](https://stackoverflow.com/questions/13451989/pythons-many-ways-of-string-formatting-are-the-older-ones-going-to-be-depre) and see examples of their use
in this [dbt-duckdb integration test](https://github.com/jwills/dbt-duckdb/blob/master/tests/functional/adapter/test_sources.py).

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
| location | [external_location](dbt/include/duckdb/macros/utils/external_location.sql) macro | The path to write the external materialization to. See below for more details.
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

Unfortunately incremental materialization strategies are not yet supported for `external` models.


#### Incremental Strategy Configuration

dbt-duckdb supports the `delete+insert`, `append`, and `merge` strategies for incremental `table` models. The `merge` strategy requires DuckDB >= 1.4.0 and provides access to DuckDB's native MERGE statement.

**Append Strategy:**

| Configuration | Type | Default | Description |
|---------------|------|---------|-------------|
| `incremental_predicates` | list | null | SQL conditions to filter which records get appended |

Example:
```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: append
      incremental_predicates: ["created_at > (select max(created_at) from {{ this }})"]
```

**Delete+Insert Strategy:**

| Configuration | Type | Default | Description |
|---------------|------|---------|-------------|
| `unique_key` | string/list | required | Column(s) used to identify records for deletion |
| `incremental_predicates` | list | null | SQL conditions to filter the delete and insert operations |

Example:
```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: delete+insert
      unique_key: id  # or ['id', 'date'] for composite keys
      incremental_predicates: ["updated_at >= '2023-01-01'"]
```

**Merge Strategy (DuckDB >= 1.4.0):**

The merge strategy leverages DuckDB's native MERGE statement to efficiently synchronize data between your incremental model and the target table. This strategy offers three configuration approaches: basic configuration (using simple options), enhanced configuration with explicit column control, and fully custom merge clauses.

**Basic Configuration (Default Behavior):**

When you specify only `unique_key`, dbt-duckdb uses DuckDB's `UPDATE BY NAME` and `INSERT BY NAME` operations, which automatically match columns by name between source and target tables.

```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: id  # or ['id', 'date'] for composite keys
```

This generates SQL equivalent to:
```sql
MERGE INTO target AS DBT_INTERNAL_DEST
USING source AS DBT_INTERNAL_SOURCE
ON (DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id)
WHEN MATCHED THEN UPDATE BY NAME
WHEN NOT MATCHED THEN INSERT BY NAME
```

**Enhanced Configuration:**

These options extend the basic merge behavior with additional control over which records get updated or inserted, which columns are affected, and how values are set.

| Configuration | Type | Default | Description |
|---------------|------|---------|-------------|
| `unique_key` | string/list | required | Column(s) used for the MERGE join condition |
| `incremental_predicates` | list | null | Additional SQL conditions to filter the MERGE operation |
| `merge_on_using_columns` | list | null | Columns for USING clause syntax instead of ON for the join condition |
| `merge_update_condition` | string | null | SQL condition to control when matched records are updated |
| `merge_insert_condition` | string | null | SQL condition to control when unmatched records are inserted |
| `merge_update_columns` | list | null | Specific columns to update |
| `merge_exclude_columns` | list | null | Columns to exclude from updates |
| `merge_update_set_expressions` | dict | null | Custom expressions for column updates |
| `merge_returning_columns` | list | null | Columns to return from the MERGE operation |

**Example with Enhanced Options:**
```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: id
      merge_update_condition: "DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age"
      merge_insert_condition: "DBT_INTERNAL_SOURCE.status != 'inactive'"
      merge_update_columns: ['name', 'age', 'status']
      merge_exclude_columns: ['created_at']
      merge_update_set_expressions:
        updated_at: "CURRENT_TIMESTAMP"
        version: "COALESCE(DBT_INTERNAL_DEST.version, 0) + 1"
```

**Custom Merge Clauses:**

For maximum flexibility, use `merge_clauses` to define custom `when_matched` and `when_not_matched` behaviors.  This is especially helpful in more complex scenarios where you have more than one action, multiple conditions, or error handling within a `when_matched` or `when_not_matched` clause.

*Supported When Matched Actions and Modes:*
- `update`: Update the matched record
  - `mode: by_name`: Use `UPDATE BY NAME` (default)
  - `mode: by_position`: Use `UPDATE BY POSITION`
  - `mode: star`: Use `UPDATE SET *`
  - `mode: explicit`: Use explicit column list with custom expressions
    - `update.include`: List of columns to include in the update
    - `update.exclude`: List of columns to exclude from the update
    - `update.set_expressions`: Dictionary of column-to-expression mappings for custom update values
- `delete`: Delete the matched record
- `do_nothing`: Skip the matched record
- `error`: Raise an error for matched records
  - `error_message`: Optional custom error message

*Supported When Not Matched Actions and Modes:*
- `insert`: Insert the unmatched record
  - `mode: by_name`: Use `INSERT BY NAME` (default)
  - `mode: by_position`: Use `INSERT BY POSITION`
  - `mode: star`: Use `INSERT *`
  - `mode: explicit`: Use explicit column and value lists
    - `insert.columns`: List of column names for the INSERT statement
    - `insert.values`: List of values/expressions corresponding to the columns
- `update`: Update unmatched records (for WHEN NOT MATCHED BY SOURCE scenarios)
  - `set_expressions`: Dictionary of column-to-expression mappings
- `delete`: Delete unmatched records
- `do_nothing`: Skip the unmatched record
- `error`: Raise an error for unmatched records
  - `error_message`: Optional custom error message

**Example with Custom Merge Clauses:**

```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: id
      merge_clauses:
        when_matched:
          - action: update
            mode: explicit
            condition: "DBT_INTERNAL_SOURCE.status = 'active'"
            update:
              include: ['name', 'email', 'status']
              exclude: ['created_at']
              set_expressions:
                updated_at: "CURRENT_TIMESTAMP"
                version: "COALESCE(DBT_INTERNAL_DEST.version, 0) + 1"
          - action: delete
            condition: "DBT_INTERNAL_SOURCE.status = 'deleted'"
        when_not_matched:
          - action: insert
            mode: explicit
            insert:
              columns: ['id', 'name', 'email', 'created_at']
              values: ['DBT_INTERNAL_SOURCE.id', 'DBT_INTERNAL_SOURCE.name', 'DBT_INTERNAL_SOURCE.email', 'CURRENT_TIMESTAMP']
```

**DuckLake Restrictions:**

When using DuckLake (attached DuckLake databases), MERGE statements are limited to a single UPDATE or DELETE action in `when_matched` clauses due to DuckLake's current MERGE implementation constraints.

**Table Aliases:**

In conditions and expressions, use these table aliases:
- `DBT_INTERNAL_SOURCE`: References the incoming data (your model's SELECT)
- `DBT_INTERNAL_DEST`: References the existing target table

#### Re-running external models with an in-memory version of dbt-duckdb
When using `:memory:` as the DuckDB database, subsequent dbt runs can fail when selecting a subset of models that depend on external tables. This is because external files are only registered as  DuckDB views when they are created, not when they are referenced. To overcome this issue we have provided the `register_upstream_external_models` macro that can be triggered at the beginning of a run. To enable this automatic registration, place the following in your `dbt_project.yml` file:

```yaml
on-run-start:
  - "{{ register_upstream_external_models() }}"
```

### `table_function` Materialization

dbt-duckdb also provides a custom table_function materialization to use DuckDB's Table Function / Table Macro feature to provide parameterized views.

Why use this materialization?
* Late binding of functions means that the underlying table can change (have new columns added) and the function does not need to be recreated.
  * (With a view, the create view statement would need to be re-run).
  * This allows for skipping parts of the dbt DAG, even if the underlying table changed.
* Parameters can force filter pushdown
* Functions can provide advanced features like dynamic SQL (the query and query_table functions)


Example table_function creation with 0 parameters:
```sql
{{
    config(
        materialized='table_function'
    )
}}
select * from {{ ref("example_table") }}
```

Example table_function invocation (note the parentheses are needed even with 0 parameters!):
```sql
select * from {{ ref("my_table_function") }}()
```

Example table_function creation with 2 parameters:
```sql
{{
    config(
        materialized='table_function',
        parameters=['where_a', 'where_b']
    )
}}
select *
from {{ ref("example_table") }}
where 1=1
    and a = where_a
    and b = where_b
```

Example table_function with 2 parameters invocation:
```sql
select * from {{ ref("my_table_function_with_parameters") }}(1, 2)
```

### Python Support

dbt added support for [Python models in version 1.3.0](https://docs.getdbt.com/docs/build/python-models). For most data platforms,
dbt will package up the Python code defined in a `.py` file and ship it off to be executed in whatever Python environment that
data platform supports (e.g., Snowpark for Snowflake or Dataproc for BigQuery.) In dbt-duckdb, we execute Python models in the same
process that owns the connection to the DuckDB database, which by default, is the Python process that is created when you run dbt.
To execute the Python model, we treat the `.py` file that your model is defined in as a Python module and load it into the
running process using [importlib](https://docs.python.org/3/library/importlib.html). We then construct the arguments to the `model`
function that you defined (a `dbt` object that contains the names of any `ref` and `source` information your model needs and a
`DuckDBPyConnection` object for you to interact with the underlying DuckDB database), call the `model` function, and then materialize
the returned object as a table in DuckDB.

The value of the `dbt.ref` and `dbt.source` functions inside of a Python model will be a [DuckDB Relation](https://duckdb.org/docs/api/python/reference/)
object that can be easily converted into a Pandas/Polars DataFrame or an Arrow table. The return value of the `model` function can be
any Python object that DuckDB knows how to turn into a table, including a Pandas/Polars `DataFrame`, a DuckDB `Relation`, or an Arrow `Table`,
`Dataset`, `RecordBatchReader`, or `Scanner`.

#### Batch processing with Python models

As of version 1.6.1, it is possible to both read and write data in chunks, which allows for larger-than-memory
datasets to be manipulated in Python models. Here is a basic example:
```
import pyarrow as pa

def batcher(batch_reader: pa.RecordBatchReader):
    for batch in batch_reader:
        df = batch.to_pandas()
        # Do some operations on the DF...
        # ...then yield back a new batch
        yield pa.RecordBatch.from_pandas(df)

def model(dbt, session):
    big_model = dbt.ref("big_model")
    batch_reader = big_model.record_batch(100_000)
    batch_iter = batcher(batch_reader)
    return pa.RecordBatchReader.from_batches(batch_reader.schema, batch_iter)
```

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

### Interactive Shell

As of version 1.9.3, dbt-duckdb includes an interactive shell that allows you to run dbt commands and query the DuckDB database in an integrated CLI environment. The shell automatically launches the [DuckDB UI](https://duckdb.org/2025/03/12/duckdb-ui.html), providing a visual interface to explore your data while working with your dbt models.

To start the interactive shell, use:

```
python -m dbt.adapters.duckdb.cli
```

You can specify a profile to use with the `--profile` flag:

```
python -m dbt.adapters.duckdb.cli --profile my_profile
```

The shell provides access to all standard dbt commands:
- `run` - Run dbt models
- `test` - Run tests on dbt models
- `build` - Build and test dbt models
- `seed` - Load seed files
- `snapshot` - Run snapshots
- `compile` - Compile models without running them
- `parse` - Parse the project
- `debug` - Debug connection
- `deps` - Install dependencies
- `list` - List resources

When you launch the shell, it automatically:
1. Runs `dbt debug` to test your connection
2. Parses your dbt project
3. Launches the DuckDB UI for visual data exploration

The shell supports model name autocompletion if you install the optional `iterfzf` package:

```
pip install iterfzf
```

Example workflow:
1. Start the interactive shell
2. View your project's models in the launched DuckDB UI
3. Run `build` to build your models
4. Immediately see the results in the UI and continue iterating

This interactive environment makes it easier to develop and test dbt models while simultaneously exploring the data in a visual interface.

### Roadmap

Things that we would like to add in the near future:

* Support for Delta and Iceberg external table formats (both as sources and destinations)
* Make dbt's incremental models and snapshots work with external materializations
