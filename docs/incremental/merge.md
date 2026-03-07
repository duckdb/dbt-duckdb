# Merge Strategy

The `merge` incremental strategy uses DuckDB's native MERGE statement to synchronize data between your model's SELECT output and the target table. Requires DuckDB >= 1.4.0.

## Table aliases

In all conditions and expressions, reference the two sides of the merge using these aliases:

- `DBT_INTERNAL_SOURCE` — the incoming data (your model's SELECT result)
- `DBT_INTERNAL_DEST` — the existing target table

---

## Basic configuration

Specify only `unique_key` to get the default behavior: update matched rows by name, insert unmatched rows by name.

```yaml
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: id          # or ['id', 'date'] for composite keys
```

This generates:

```sql
MERGE INTO target AS DBT_INTERNAL_DEST
USING source AS DBT_INTERNAL_SOURCE
ON (DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id)
WHEN MATCHED THEN UPDATE BY NAME
WHEN NOT MATCHED THEN INSERT BY NAME
```

---

## Enhanced configuration

These options extend the basic merge with additional control:

| Configuration | Type | Default | Description |
| --- | --- | --- | --- |
| `unique_key` | string / list | required | Column(s) for the MERGE join condition |
| `incremental_predicates` | list | null | Additional SQL conditions to filter the MERGE |
| `merge_on_using_columns` | list | null | Columns for USING clause syntax instead of ON |
| `merge_update_condition` | string | null | Condition controlling when matched rows are updated |
| `merge_insert_condition` | string | null | Condition controlling when unmatched rows are inserted |
| `merge_update_columns` | list | null | Specific columns to update (instead of all columns) |
| `merge_exclude_columns` | list | null | Columns to exclude from updates |
| `merge_update_set_expressions` | dict | null | Custom expressions for column updates |
| `merge_returning_columns` | list | null | Columns to return from the MERGE operation |

### Example

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

---

## Custom merge clauses

For maximum flexibility, use `merge_clauses` to define arbitrary `when_matched` and `when_not_matched` behaviors. This is most useful when you need multiple conditions, multiple actions, or error handling within a single clause.

### When matched actions

| Action | Description |
| --- | --- |
| `update` | Update the matched row |
| `delete` | Delete the matched row |
| `do_nothing` | Skip the matched row |
| `error` | Raise an error for matched rows (set `error_message` for a custom message) |

Update modes for `action: update`:

| Mode | SQL generated |
| --- | --- |
| `by_name` (default) | `UPDATE BY NAME` |
| `by_position` | `UPDATE BY POSITION` |
| `star` | `UPDATE SET *` |
| `explicit` | Explicit column list with optional custom expressions |

For `mode: explicit`, you can also set:
- `update.include` — list of columns to include
- `update.exclude` — list of columns to exclude
- `update.set_expressions` — dict of column → expression mappings

### When not matched actions

| Action | Description |
| --- | --- |
| `insert` | Insert the unmatched row |
| `update` | Update unmatched rows (for WHEN NOT MATCHED BY SOURCE) |
| `delete` | Delete unmatched rows |
| `do_nothing` | Skip the unmatched row |
| `error` | Raise an error for unmatched rows |

Insert modes for `action: insert`:

| Mode | SQL generated |
| --- | --- |
| `by_name` (default) | `INSERT BY NAME` |
| `by_position` | `INSERT BY POSITION` |
| `star` | `INSERT *` |
| `explicit` | Explicit columns and values lists |

### Example

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
              values:
                - 'DBT_INTERNAL_SOURCE.id'
                - 'DBT_INTERNAL_SOURCE.name'
                - 'DBT_INTERNAL_SOURCE.email'
                - 'CURRENT_TIMESTAMP'
```

---

## DuckLake restrictions

When using a DuckLake attached database, MERGE statements are limited to a **single UPDATE or DELETE action** in `when_matched` clauses due to DuckLake's current MERGE implementation constraints.
