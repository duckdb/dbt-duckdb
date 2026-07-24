1.10.1 (2026-02-17)
-------------------

- Dropped support for Python 3.8 and 3.9
- Added microbatch incremental strategy support
- Fixed transaction rollback in exception handler
- Fixed transaction commit error when no upstream external models exist
- Improved DuckDB error message logging

1.10.0 (2025-11-05)
-------------------

- Added MERGE incremental strategy with DuckDB extensions support
- Added Python 3.13 support
- Fixed excel plugin `skip_empty_sheets` handling
- Added DuckLake connection instructions
- Added Slack notification on CI failure

1.9.6 (2025-09-08)
------------------

- Removed extraneous log lines for index-related operations

1.9.5 (2025-09-08)
------------------

- Added index support for dbt-duckdb models
- Added struct column support with flatten functionality
- Added DuckLake support as primary database type
- Fixed incremental Python models on MotherDuck
- Fixed `dateadd` interval syntax for newer DuckDB versions
- Fixed secrets map/list type handling in SQL generation
- Fixed orchestrator compatibility by changing logger.exception to logger.warning
- Updated Excel plugin to filter all null rows
- Bumped DuckDB to 1.3.2

1.9.4 (2025-06-25)
------------------

- Applied project quoting setting to add/remove columns
- Added named secret parameter to attachment class for profiles
- Added support for arbitrary key-value pairs in Attachment ATTACH options
- Updated macros for dropping schema and relations when using DuckLake
- Fixed external materializations with column names containing spaces
- Fixed attachment option quoting to prevent SQL errors with path-like values
- Fixed bug where schema's database was not specified
- Fixed error swallowing that hid DuckDB exception details
- Fixed write conflict when first creating `dbt_temp` schema
- Bumped DuckDB to 1.3.1

1.9.3 (2025-04-21)
------------------

- Added `table_function` materialization (skip view recreate on schema change)
- Added support for scoped secrets/credentials by storage prefix
- Added `dbt_valid_to_current` support for snapshots
- Fixed external materialization strategy adding extra newlines to logs
- Fixed: turned transactions back on for MotherDuck
- Bumped DuckDB to 1.2.2

1.9.2 (2025-02-12)
------------------

- Dropped Python 3.8 support, added Python 3.12
- Made `keep_open: true` the default setting
- Added support for `per_thread_output` in external materialization
- Fixed external materialization to S3 for empty tables

1.9.1 (2024-12-05)
------------------

- Added HTTP secret `extra_http_headers` support
- Added transaction support for MotherDuck
- Made grant configs a no-op for DuckDB
- Added basic AWS Glue struct datatype support and JSON/CSV/Parquet read options
- Moved secret creation to connection-level instead of cursor-level
- Added safety check for node in graph
- Updated Postgres plugin to use ATTACH syntax
- Added unique UUID to temp table name for concurrent incremental models

1.9.0 (2024-10-07)
------------------

- Updated for dbt-core 1.9.0
- Fixed Excel logger compatibility with dbt-core 1.9.0
- Fixed `generate_database_name` macro to be case insensitive
- Added support for configuring community/nightly extensions in the profile
- Fixed pre and post-model hooks for dbt-adapters 1.9.0 on MotherDuck

1.8.4 (2024-09-27)
------------------

- Added DuckDB 1.1.0 compatibility fixes
- Disabled Python models on MotherDuck when SaaS mode is on
- Removed deprecated DeltaLake functions
- Fixed MotherDuck token from plugin config to override env var
- Added support for attaching a MotherDuck database with token in settings
- Added support for pre-release versions of DuckDB

1.8.3 (2024-08-19)
------------------

- Fixed secrets value quoting in SQL generation
- Switched to pbr for automated versioning with git tags

1.8.2 (2024-07-24)
------------------

- Bumped DuckDB to 1.0.0
- Added support for new DuckDB Secrets Manager (deprecated old AWS credentials chain)
- Fixed limit 0 clauses to not require aliases

1.8.1 (2024-05-30)
------------------

- Pinned to DuckDB v0.10.2 to avoid view dependency issues

1.8.0 (2024-05-10)
------------------

- Migrated to dbt-common and dbt-adapters packages

1.7.5 (2024-05-08)
------------------

- Added query cancellation support
- Added `profile_name` to boto3 Session for AWS credential configuration

1.7.4 (2024-04-17)
------------------

- Added support for `persist_docs` functionality
- Lazy load agate for faster startup
- Added pre-model hook for cleaning up remote temporary tables on MotherDuck

1.7.3 (2024-03-07)
------------------

- Fixed `dateadd` macro to work with updated `datediff`
- Fixed `custom_user_agent` to run after connect
- Fixed `custom_user_agent` when MotherDuck plugin is not specified in profiles.yml
- Added automated release pipeline

1.7.2 (2024-02-22)
------------------

- Added partition support for Glue
- Added workaround for temporary tables in remote database for incremental models
- Added `custom_user_agent` to MotherDuck connection config
- Switched to DuckDB's built-in `date_diff` function for the `datediff` macro
- Fixed MotherDuck token from plugin config to override env var
- Added support for dynamically registering/adding partitions

1.7.1 (2024-01-13)
------------------

- Added support for additional kwargs for SQLAlchemy connections
- Fixed SQLite write operations
- Updated `is_integer` and `is_float` for DuckDB integer/float types
- Moved from black/flake8 to ruff for linting
- Added support for retrying certain exceptions during model runs

1.7.0 (2023-11-06)
------------------

- Updated for dbt-core 1.7.0

1.6.2 (2023-10-26)
------------------

- Re-enabled returning a temp table in Python models
- Added Excel plugin output (write) support
- Added support for reading Delta tables with the delta plugin

1.6.1 (2023-10-14)
------------------

- Made source tags available to plugins through the SourceConfig
- Added S3 support to Excel plugin through env variables
- Added option to fetch Google Sheets data using range
- Added batch processing support for Python models
- Fixed cursor isolation on Python models
- Allowed access to RuntimeConfigObject from TargetConfig

1.6.0 (2023-08-02)
------------------

- Updated for dbt-core 1.6.0 with revamped `dbt debug` command support
- Added plugins for MotherDuck and Postgres extensions
- Added `keep_open` option to the profile
- Required DuckDB >= 0.7.0 going forward
- Made fast seed loading the default
- Added `module_path` option for specifying additional `sys.path` entries

1.5.2 (2023-06-22)
------------------

- Added fast-path option for loading seed files via DuckDB COPY
- Created plugin system for writing data to external destinations
- Added support for broader incremental functionality
- Added MotherDuck database support with automatic transaction disabling
- Simplified extension loading
- Revamped plugins to support configuring DuckDBPyConnection directly
- Fixed catalog lookups in multi-database, multi-schema environments
- Dropped Python 3.7 support

1.5.1 (2023-05-05)
------------------

- Added support for using `source.config` entries for external data
- Fixed in-memory database setup for minimal profiles (`type: duckdb` only)
- Made pyarrow optional for Python models
- Fixed connection release when environment is not in use

1.5.0 (2023-04-27)
------------------

- Updated for dbt-core 1.5.0 with support for model contracts
- Smarter handling of the profile `path` argument

1.4.2 (2023-04-26)
------------------

- Refactored `connections` module into `credentials` and `environments` modules
- Created a simple plugin system for loading data from external sources
- Added Iceberg config options and Spark-style `save_mode` for plugins
- Injected `meta` dictionary key-value pairs into f-string context for external locations
- Added Buena Vista remote environment support
- Enabled file-based database testing

1.4.1 (2023-03-16)
------------------

- Added support for filesystems
- Added support for arbitrary `options` to pass to DuckDB `COPY` for external materializations
- Bug fixes and documentation updates

1.4.0 (2023-02-14)
------------------

- Added support for DuckDB 0.7.x and the ability to `ATTACH` additional databases

1.3.2 (2022-11-16)
------------------

- Added support for DuckDB 0.6.x

1.3.1 (2022-11-07)
------------------

- Support for Python models in dbt-duckdb
- Support for the `external` materialization type

1.2.3 (2022-10-24)
------------------

- Added the `settings` dictionary for configuring arbitrary settings in the DuckDB
instance used during the dbt run

1.2.2 (2022-10-05)
------------------

- Fixed a small bug in the multithreading implementation

1.2.1 (2022-10-03)
------------------

- Added support for multi-threaded dbt-duckdb runs

1.2.0 (2022-09-26)
------------------

- Support for loading DuckDB extensions
- Support for reading/writing from S3 via the aforementioned extensions

1.1.4 (2022-07-06)
------------------

- Enforces the single-thread limit on the dbt-duckdb profile

1.1.3 (2022-06-29)
------------------

- Fixes DuckDB 0.4.0 compatibility issue

1.1.2 (2022-06-29)
------------------

- Align with minor version of dbt-core
- Constrain range of compatible duckdb versions

1.1.1 (2022-04-06)
------------------

- Fix typo in package description

1.1.0 (2022-04-06)
------------------

- Upgraded to DuckDB 0.3.2
- Refactored adapter so that dbt threads > 1 work with DuckDB

1.0.0 (2022-01-10)
------------------

- Upgraded to DuckDB 0.3.1
- First basically working version
