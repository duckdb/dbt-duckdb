# Incremental Models

dbt-duckdb supports four incremental strategies for `table` models:

| Strategy | Description | Minimum version |
| --- | --- | --- |
| `append` | Insert new rows without touching existing data | Any |
| `delete+insert` | Delete matching rows, then insert replacements | Any |
| `microbatch` | Time-windowed batch processing with per-batch delete+insert | dbt-core >= 1.9 |
| `merge` | Native DuckDB MERGE statement | DuckDB >= 1.4.0 |

## Append

Inserts all rows produced by the model query without modifying existing rows. Use `incremental_predicates` to filter which rows get appended:

```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: append
      incremental_predicates:
        - "created_at > (select max(created_at) from {{ this }})"
```

| Configuration | Type | Default | Description |
| --- | --- | --- | --- |
| `incremental_predicates` | list | null | SQL conditions to filter which records get appended |

## Delete+Insert

Deletes rows from the target table that match the `unique_key`, then inserts the full result set from the model query. Use `incremental_predicates` to further scope the delete and insert operations:

```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: delete+insert
      unique_key: id
      incremental_predicates:
        - "updated_at >= '2023-01-01'"
```

| Configuration | Type | Default | Description |
| --- | --- | --- | --- |
| `unique_key` | string / list | required | Column(s) used to identify records for deletion |
| `incremental_predicates` | list | null | SQL conditions to scope the delete and insert |

## Microbatch

Runs incremental builds in time-based batches using a configured `event_time` column. Each batch generates a `delete` + `insert` scoped to the batch window.

```yaml
models:
  - name: my_microbatch_model
    config:
      materialized: incremental
      incremental_strategy: microbatch
      event_time: event_time
      begin: '2025-01-01'
      batch_size: day
      incremental_predicates:
        - "country = 'US'"
```

| Configuration | Type | Default | Description |
| --- | --- | --- | --- |
| `event_time` | string | required | Timestamp column used for batch windowing |
| `begin` | string | required | Start date for batching (e.g., `YYYY-MM-DD`) |
| `batch_size` | string | required | Batch grain: `day`, `hour`, etc. |
| `incremental_predicates` | list | null | Additional predicates applied within each batch |

> **Note:** `unique_key` is not supported with `microbatch`. Microbatch performs window-scoped deletes, not key-based upserts. Use `merge` if you need key-based upserts.

> **Performance tip:** Microbatch is most efficient for physically partitioned tables (e.g., a DuckLake). DuckDB operates on row groups, not physical partitions, so increasing thread count and batch parallelism does not always improve performance. Test with your specific data layout.

## Merge

Uses DuckDB's native MERGE statement to synchronize data between the model result and the target table. Requires DuckDB >= 1.4.0.

See [Merge Strategy](merge.md) for full configuration options including enhanced merge controls and custom merge clauses.
