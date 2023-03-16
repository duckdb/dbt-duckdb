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
