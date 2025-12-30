# Requirements Document: S3 Tables Schema Evolution Without Column Drops

## Introduction

This feature ensures that when dbt models detect schema changes between source and target tables in AWS S3 Tables (Iceberg), the system handles column additions and removals correctly. Specifically, columns that are removed from the source should NOT be dropped from the target table - instead they should be preserved and populated with NULL values during INSERT operations.

## Glossary

- **S3 Tables**: AWS managed service for Apache Iceberg tables
- **PyIceberg**: Python library for interacting with Iceberg tables
- **Source Table**: Temporary table containing new data from the dbt model query
- **Target Table**: Existing Iceberg table in S3 Tables
- **Schema Evolution**: Process of modifying table schema (adding/removing columns)
- **Column Removal**: When a column exists in target but not in source
- **Column Addition**: When a column exists in source but not in target

## Requirements

### Requirement 1: Detect Schema Changes

**User Story:** As a dbt user, I want the system to detect when my model's columns differ from the target table, so that schema changes can be handled automatically.

#### Acceptance Criteria

1. WHEN the source table columns are compared to target table columns THEN the system SHALL identify columns present in source but not in target (additions)
2. WHEN the source table columns are compared to target table columns THEN the system SHALL identify columns present in target but not in source (removals)
3. WHEN comparing column names THEN the system SHALL use case-insensitive comparison to avoid false positives
4. WHEN no schema changes are detected THEN the system SHALL proceed with standard incremental logic

### Requirement 2: Handle Column Additions

**User Story:** As a dbt user, I want new columns in my model to be automatically added to the target table, so that I don't have to manually alter the schema.

#### Acceptance Criteria

1. WHEN new columns are detected in source THEN the system SHALL add those columns to the target table using PyIceberg
2. WHEN adding columns THEN the system SHALL preserve the DuckDB data type from the source table
3. WHEN adding columns THEN the system SHALL map DuckDB types to appropriate Iceberg types
4. WHEN column addition succeeds THEN the system SHALL include the new columns in the INSERT statement

### Requirement 3: Preserve Columns Removed from Source (NO DROP)

**User Story:** As a dbt user, I want columns that are removed from my model to remain in the target table with NULL values, so that I don't lose historical data or break downstream dependencies.

#### Acceptance Criteria

1. WHEN columns exist in target but not in source THEN the system SHALL NOT drop those columns from the target table
2. WHEN columns exist in target but not in source THEN the system SHALL NOT call PyIceberg drop_columns method
3. WHEN inserting data THEN the system SHALL include removed columns in the INSERT statement
4. WHEN inserting data THEN the system SHALL SELECT NULL for columns that don't exist in source
5. WHEN inserting data THEN the system SHALL SELECT actual values for columns that exist in source

### Requirement 4: Calculate Target Columns Correctly

**User Story:** As a dbt user, I want the system to correctly calculate the final column list after schema evolution, so that INSERT statements work without errors.

#### Acceptance Criteria

1. WHEN schema changes are detected THEN the system SHALL calculate refreshed columns as: old_target_columns + new_columns
2. WHEN calculating refreshed columns THEN the system SHALL NOT subtract removed columns
3. WHEN building INSERT column list THEN the system SHALL use all refreshed columns
4. WHEN building SELECT statement THEN the system SHALL map each refreshed column to either source column or NULL

### Requirement 5: Build Correct INSERT Statement

**User Story:** As a dbt user, I want the INSERT statement to correctly handle all columns (existing, new, and removed), so that data loads successfully.

#### Acceptance Criteria

1. WHEN building INSERT statement THEN the system SHALL include all target columns (old + new)
2. WHEN building SELECT clause THEN the system SHALL SELECT source columns for columns present in source
3. WHEN building SELECT clause THEN the system SHALL SELECT NULL AS "column" for columns not present in source
4. WHEN executing INSERT THEN the system SHALL use explicit column lists to avoid column order issues
5. WHEN executing INSERT THEN the system SHALL ensure column count matches between INSERT and SELECT

### Requirement 6: Handle Metadata Refresh Issues

**User Story:** As a dbt user, I want the system to work around DuckDB metadata caching issues, so that schema changes are immediately visible.

#### Acceptance Criteria

1. WHEN schema evolution completes THEN the system SHALL calculate refreshed columns mathematically without querying DESCRIBE
2. WHEN calculating refreshed columns THEN the system SHALL use: (old_columns - []) + new_columns
3. WHEN building SELECT statement THEN the system SHALL use source column list to determine which columns exist
4. WHEN a column exists in target but not source THEN the system SHALL generate NULL AS "column" in SELECT

### Requirement 7: Maintain Transaction Safety

**User Story:** As a dbt user, I want schema evolution and data insertion to be atomic, so that failures don't leave the table in an inconsistent state.

#### Acceptance Criteria

1. WHEN schema evolution fails THEN the system SHALL raise an error and halt execution
2. WHEN DELETE and INSERT are executed THEN the system SHALL combine them in a single statement for atomicity
3. WHEN INSERT fails THEN the system SHALL ensure DELETE is rolled back automatically
4. WHEN all operations succeed THEN the system SHALL commit the transaction

## Example Scenario

### Initial State
- Target table: `[customerid, name, placeholder5]`
- Source query returns: `[customerid, name, placeholder6]`

### Expected Behavior
1. Detect: `placeholder6` is new (addition), `placeholder5` is removed
2. Add `placeholder6` to target via PyIceberg
3. Target becomes: `[customerid, name, placeholder5, placeholder6]`
4. Execute:
   ```sql
   DELETE FROM target WHERE customerid IN (SELECT DISTINCT customerid FROM source);
   
   INSERT INTO target (customerid, name, placeholder5, placeholder6)
   SELECT customerid, name, NULL AS placeholder5, placeholder6
   FROM source;
   ```

### Result
- Target has 4 columns
- `placeholder5` contains NULL for new rows (not dropped)
- `placeholder6` contains values from source
- Historical rows with `placeholder5` values are preserved
