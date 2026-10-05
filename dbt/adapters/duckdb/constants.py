TEMP_SCHEMA_NAME = "temp_schema_name"
DEFAULT_TEMP_SCHEMA_NAME = "dbt_temp"
DUCKDB_MERGE_LOWEST_VERSION_POSSIBLE = "1.4.0-dev0"
DUCKLAKE_ALTER_RENAME_FIX_VERSION = "1.5.3"
DUCKDB_BASE_INCREMENTAL_STRATEGIES = ["append", "delete+insert"]

# Where a Python model's body runs: in the dbt process, or on a MotherDuck Flight
LOCAL_SUBMISSION = "local"
FLIGHT_SUBMISSION = "flight"
SUBMISSION_METHODS = (LOCAL_SUBMISSION, FLIGHT_SUBMISSION)

# Key under which the adapter passes the flight_name macro's result to the environment
FLIGHT_NAME_KEY = "__dbt_duckdb_flight_name"
