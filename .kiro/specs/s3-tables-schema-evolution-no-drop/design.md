# Design Document: S3 Tables Schema Evolution Without Column Drops

## Overview

This design implements schema evolution for AWS S3 Tables (Iceberg) in dbt-duckdb that preserves all target table columns. When columns are removed from the source model, they remain in the target table and receive NULL values during INSERT operations. This approach prevents data loss and maintains backward compatibility with downstream systems.

## Architecture

### High-Level Flow

```
1. Detect schema changes (compare source vs target columns)
2. IF new columns exist:
   - Add new columns to target via PyIceberg
3. Calculate final target columns = old_target_columns + new_columns
4. Build INSERT statement:
   - INSERT INTO target (all_target_columns)
   - SELECT: source_columns + NULL for removed_columns
5. Execute atomic DELETE + INSERT
```

### Key Principle

**Never drop columns from target table.** Columns that disappear from source are preserved in target with NULL values.

## Components and Interfaces

### 1. Schema Change Detection (Jinja Macro)

**Location:** `dbt/include/duckdb/macros/materializations/incremental.sql`

**Inputs:**
- `target_cols_raw`: List of column names from target table (via DESCRIBE)
- `source_cols`: List of column names from source table (via DESCRIBE)

**Outputs:**
- `new_columns`: Columns in source but not in target
- `removed_columns`: Columns in target but not in source

**Logic:**
```jinja
{# Case-insensitive comparison #}
{%- for col in source_cols -%}
  {%- if (col | lower) not in target_cols_lower -%}
    {%- do new_columns.append(col) -%}
  {%- endif -%}
{%- endfor -%}

{%- for col in target_cols_raw -%}
  {%- if (col | lower) not in source_cols_lower -%}
    {%- do removed_columns.append(col) -%}
  {%- endif -%}
{%- endfor -%}
```

### 2. Schema Evolution (Python - PyIceberg)

**Location:** `dbt/adapters/duckdb/impl.py` → `evolve_s3_tables_schema()`

**Inputs:**
- `relation`: dbt relation object
- `add_columns`: List of `{'name': str, 'type': str}` (ONLY new columns)
- `drop_columns`: MUST BE None or empty list (never drop)

**Outputs:**
- `bool`: True if successful

**Logic:**
```python
# ONLY add columns, NEVER drop
if add_columns:
    evolver.add_columns(table_identifier, add_columns)

# drop_columns should never be called
if drop_columns:
    raise Error("Column drops not supported")
```

### 3. Column List Calculation (Jinja Macro)

**Location:** `dbt/include/duckdb/macros/materializations/incremental.sql`

**Inputs:**
- `target_cols_raw`: Original target columns
- `new_columns`: Columns to add
- `removed_columns`: Columns removed from source (for NULL padding)

**Outputs:**
- `target_cols_raw_refresh`: Final target column list
- `select_cols_str`: SELECT clause with NULL padding

**Logic:**
```jinja
{# Calculate: old_columns + new_columns (NO subtraction) #}
{%- set target_cols_raw_refresh = [] -%}

{# Keep ALL old columns #}
{%- for col in target_cols_raw -%}
  {%- do target_cols_raw_refresh.append(col) -%}
{%- endfor -%}

{# Add new columns #}
{%- for col in new_columns -%}
  {%- do target_cols_raw_refresh.append(col) -%}
{%- endfor -%}

{# Build SELECT with NULL padding #}
{%- set select_cols = [] -%}
{%- for col in target_cols_raw_refresh -%}
  {%- if (col | lower) in source_cols_lower -%}
    {%- do select_cols.append('"' ~ col ~ '"') -%}
  {%- else -%}
    {%- do select_cols.append('NULL AS "' ~ col ~ '"') -%}
  {%- endif -%}
{%- endfor -%}
```

### 4. INSERT Statement Builder (Jinja Macro)

**Location:** `dbt/include/duckdb/macros/materializations/incremental.sql`

**Inputs:**
- `target_cols_raw_refresh`: All target columns (old + new)
- `select_cols_str`: SELECT clause with NULL padding

**Outputs:**
- `build_sql`: Complete DELETE + INSERT statement

**Logic:**
```jinja
{% set build_sql %}
  DELETE FROM {{ target_relation }}
  WHERE {{ unique_key }} IN (
    SELECT DISTINCT {{ unique_key }} FROM {{ staged_relation }}
  );
  
  INSERT INTO {{ target_relation }} ({{ insert_cols_str }})
  SELECT {{ select_cols_final }}
  FROM {{ staged_relation }}
{% endset %}
```

## Data Models

### Column Lists

```python
# Before schema evolution
target_cols_raw = ['customerid', 'name', 'placeholder5']
source_cols = ['customerid', 'name', 'placeholder6']

# After detection
new_columns = ['placeholder6']
removed_columns = ['placeholder5']

# After calculation (NO subtraction of removed_columns)
target_cols_raw_refresh = ['customerid', 'name', 'placeholder5', 'placeholder6']

# SELECT clause
select_cols_str = '"customerid", "name", NULL AS "placeholder5", "placeholder6"'
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Column Preservation Invariant

*For any* target table and source table, after schema evolution, all original target columns SHALL remain in the target table.

**Validates: Requirements 3.1, 3.2**

### Property 2: Column Addition Completeness

*For any* column present in source but not in target, after schema evolution, that column SHALL exist in the target table.

**Validates: Requirements 2.1, 2.4**

### Property 3: Refreshed Column Calculation

*For any* target table with columns T and new columns N, the refreshed column list SHALL equal T ∪ N (union, not difference).

**Validates: Requirements 4.1, 4.2**

### Property 4: NULL Padding Correctness

*For any* column in target but not in source, the SELECT statement SHALL include "NULL AS column_name" for that column.

**Validates: Requirements 3.5, 5.3**

### Property 5: INSERT-SELECT Column Count Match

*For any* INSERT statement, the number of columns in the INSERT clause SHALL equal the number of expressions in the SELECT clause.

**Validates: Requirements 5.5**

### Property 6: No Drop Operations

*For any* schema evolution operation, the system SHALL NOT invoke PyIceberg's drop_columns method.

**Validates: Requirements 3.2**

## Error Handling

### Schema Evolution Failures

- **PyIceberg connection failure**: Raise clear error with catalog configuration details
- **Column addition failure**: Raise error and halt execution (don't proceed to INSERT)
- **Type mapping failure**: Log warning and default to StringType

### INSERT Failures

- **Column count mismatch**: Should never occur due to explicit column lists
- **NULL constraint violation**: User error - removed column had NOT NULL constraint
- **Type mismatch**: Should never occur - PyIceberg handles type compatibility

## Testing Strategy

### Unit Tests

1. **Test column detection logic**
   - Input: target=['a', 'b', 'c'], source=['a', 'b', 'd']
   - Expected: new=['d'], removed=['c']

2. **Test case-insensitive comparison**
   - Input: target=['CustomerID'], source=['customerid']
   - Expected: new=[], removed=[]

3. **Test refreshed column calculation**
   - Input: old=['a', 'b'], new=['c'], removed=['b']
   - Expected: refreshed=['a', 'b', 'c'] (NOT ['a', 'c'])

4. **Test NULL padding generation**
   - Input: target=['a', 'b', 'c'], source=['a', 'c']
   - Expected: SELECT "a", NULL AS "b", "c"

### Property-Based Tests

1. **Property 1: Column preservation**
   - Generate random target columns and source columns
   - Verify all target columns remain after evolution

2. **Property 3: Refreshed calculation**
   - Generate random old columns and new columns
   - Verify refreshed = old + new (no subtraction)

3. **Property 4: NULL padding**
   - Generate random target and source columns
   - Verify SELECT has NULL for every column in target but not source

4. **Property 5: Column count match**
   - Generate random column lists
   - Verify len(INSERT columns) == len(SELECT expressions)

### Integration Tests

1. **End-to-end schema evolution**
   - Create target table with columns [a, b, c]
   - Run model with columns [a, b, d]
   - Verify target has [a, b, c, d]
   - Verify new rows have NULL for column c

2. **Multiple evolution cycles**
   - Run 1: [a, b] → [a, b]
   - Run 2: [a, b] → [a, c] (add c, remove b)
   - Run 3: [a, c] → [a, d] (add d, remove c)
   - Verify final table has [a, b, c, d]

## Implementation Notes

### Critical Changes Required

1. **Remove drop_columns call**
   - Current: Calls `adapter.evolve_s3_tables_schema()` with `drop_columns=removed_columns`
   - Fixed: Call with `drop_columns=None` or don't pass parameter

2. **Fix column calculation**
   - Current: `refreshed = (old - removed) + new`
   - Fixed: `refreshed = old + new`

3. **Keep NULL padding logic**
   - Current implementation is correct
   - SELECT NULL for columns not in source

### Metadata Refresh Workaround

DuckDB caches Iceberg metadata, so DESCRIBE after PyIceberg changes may return stale data. Solution: Calculate refreshed columns mathematically instead of querying.

### Performance Considerations

- NULL columns consume minimal storage in Iceberg (columnar format)
- No performance impact on queries that don't reference removed columns
- Slight overhead in INSERT statements (additional NULL values)

## Dependencies

- **PyIceberg >= 0.5.0**: For schema evolution via REST API
- **DuckDB >= 1.4.0**: For Iceberg support
- **AWS credentials**: For S3 Tables access via IAM role or environment variables
