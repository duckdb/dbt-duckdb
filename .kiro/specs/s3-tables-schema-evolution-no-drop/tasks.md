# Implementation Plan: S3 Tables Schema Evolution Without Column Drops

## Tasks

- [x] 1. Fix column calculation logic in incremental.sql


  - Remove subtraction of removed_columns from target_cols_raw_refresh
  - Change calculation to: old_columns + new_columns (keep ALL old columns)
  - Update debug logging to show correct calculation
  - _Requirements: 4.1, 4.2, 4.3_



- [ ] 2. Remove drop_columns call to PyIceberg
  - Change evolve_s3_tables_schema call to NOT pass drop_columns parameter
  - Or pass drop_columns=None explicitly


  - Remove the drop_columns logic from being sent to PyIceberg
  - _Requirements: 3.1, 3.2_

- [x] 3. Verify NULL padding logic is correct




  - Ensure SELECT statement includes NULL AS "column" for removed columns
  - Verify logic checks if column exists in source_cols_lower
  - Confirm NULL padding works for all removed columns
  - _Requirements: 3.5, 5.3_

- [ ] 4. Test the complete flow
  - Reinstall dbt-duckdb: `pip install .`
  - Run dbt model: `dbt run --select stg_cust`
  - Verify target table has old + new columns
  - Verify removed columns have NULL values
  - Verify new columns have source values
  - _Requirements: All_

- [ ] 5. Clean up debug logging
  - Remove excessive DEBUG logs once confirmed working
  - Keep essential logs for schema evolution events
  - _Requirements: N/A_
