models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_a', 'column_b'], 'unique': True},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__table_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_b']},
      {'columns': ['column_a', 'column_b']},
      {'columns': ['column_b', 'column_a'], 'unique': True},
      {'columns': ['column_a']}
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

snapshots__colors_sql = """
{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id']},
              {'columns': ['id', 'color'], 'unique': True},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% else %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% endif %}

{% endsnapshot %}

"""

seeds__seed_csv = """country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""

# Regression for #919: DuckDB will not ALTER a table that has an index, so an
# incremental model with `indexes` and `on_schema_change='sync_all_columns'`
# failed on the run where a column was removed or changed type. The `note`
# index is dropped from the config on the incremental run (via the
# `drop_note` var): under the config-driven rebuild, an index whose column
# is gone cannot be recreated, so the config must stop listing it -- the
# same as on a full refresh.
models__incremental_sync_all_columns_remove_sql = """
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['id']},
      {'columns': ['note']},
    ] if not var('drop_note', false) else [
      {'columns': ['id']},
    ]
  )
}}

{% if is_incremental() %}

select 2 as id, 20 as amount

{% else %}

select 1 as id, 10 as amount, 'x' as note

{% endif %}
"""

# Same as the remove case, but the index on the removed column is unique --
# it must not be brought back when it is taken out of the config.
models__incremental_sync_all_columns_remove_unique_sql = """
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['note'], 'unique': True},
    ] if not var('drop_note', false) else [
      {'columns': ['id'], 'unique': True},
    ]
  )
}}

{% if is_incremental() %}

select 2 as id, 20 as amount

{% else %}

select 1 as id, 10 as amount, 'x' as note

{% endif %}
"""

# Add-only sync: adding a column must leave the indexes alone.
models__incremental_sync_all_columns_add_sql = """
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['id']},
    ]
  )
}}

{% if is_incremental() %}

select 2 as id, 20 as amount, 'y' as note

{% else %}

select 1 as id, 10 as amount

{% endif %}
"""

# Same as the failing remove case, but for a type change: the ALTER runs
# through a statement block (new transaction), so it rolls back with the
# failed run and the table is left without its indexes.
models__incremental_sync_all_columns_failing_type_change_sql = """
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['id']},
      {'columns': ['amount']},
    ],
    post_hook=('select * from this_does_not_exist' if var('fail_after_sync', false) else 'select 1')
  )
}}

{% if is_incremental() %}

select 2 as id, 'twenty' as amount

{% else %}

select 1 as id, 10 as amount

{% endif %}
"""

# The model fails after the sync (failing post-hook, switched on with the
# `fail_after_sync` var) to exercise what the committed index drop leaves
# behind when a later step fails.
models__incremental_sync_all_columns_failing_sql = """
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['id']},
      {'columns': ['note']},
    ] if not var('drop_note', false) else [
      {'columns': ['id']},
    ],
    post_hook=('select * from this_does_not_exist' if var('fail_after_sync', false) else 'select 1')
  )
}}

{% if is_incremental() %}

select 2 as id, 20 as amount

{% else %}

select 1 as id, 10 as amount, 'x' as note

{% endif %}
"""

models__incremental_sync_all_columns_type_change_sql = """
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['id']},
      {'columns': ['amount']},
    ]
  )
}}

{% if is_incremental() %}

select 2 as id, 'twenty' as amount

{% else %}

select 1 as id, 10 as amount

{% endif %}
"""
