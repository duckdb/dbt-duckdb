# Table Function Materialization

dbt-duckdb provides a `table_function` materialization that leverages DuckDB's [Table Function / Table Macro](https://duckdb.org/docs/sql/functions/table_functions.html) feature to create parameterized views.

## Why use table functions?

- **Late binding** — the underlying table can add new columns without needing to recreate the function, unlike regular views. This allows you to skip parts of the dbt DAG even when upstream tables change.
- **Parameter-driven filter pushdown** — parameters can be used to force predicate pushdown into the query.
- **Advanced SQL features** — access DuckDB features like `query` and `query_table` for dynamic SQL.

## Zero-parameter table function

```sql
{{
    config(
        materialized='table_function'
    )
}}
select * from {{ ref("example_table") }}
```

Invoke it in another model (note the parentheses are required even with zero parameters):

```sql
select * from {{ ref("my_table_function") }}()
```

## Table function with parameters

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

Pass arguments by position when invoking:

```sql
select * from {{ ref("my_table_function_with_parameters") }}(1, 2)
```
