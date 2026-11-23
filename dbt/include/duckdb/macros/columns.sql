{% macro duckdb__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns %}
    {% for column in add_columns %}
      {% set sql -%}
         alter {{ relation.type }} {{ relation }} add column
           {{ api.Relation.create(identifier=column.name) }} {{ column.data_type }}
      {%- endset -%}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}

  {% if remove_columns %}
    {% for column in remove_columns %}
      {% set sql -%}
        alter {{ relation.type }} {{ relation }} drop column
          {{ api.Relation.create(identifier=column.name) }}
      {%- endset -%}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro duckdb__alter_column_type(relation, column_name, new_column_type) -%}
  {#-- The default implementation adds a column, copies the data into it, drops the
       old column and renames the new one. DuckDB cannot commit a transaction that
       both updates a table's rows and alters its schema ("Attempting to modify table
       ... but another transaction has altered this table"), so change the type in a
       single ALTER instead, which DuckDB casts in place. --#}
  {% call statement('alter_column_type') %}
    alter table {{ relation.render() }} alter column {{ adapter.quote(column_name) }} type {{ new_column_type }}
  {% endcall %}
{%- endmacro %}

{% macro indexes_name_changed_columns(relation, changed_columns) -%}
  {#-- Whether any index on `relation` names one of the `changed_columns`
       (lower-cased names) in its `expressions` as recorded by
       duckdb_indexes(). `expressions` is a string like `'[a, b]'`, or
       `"['(lower(b))']"` for an expression index, so it is normalized to
       words before comparing: a plain column must match a whole word, and
       an expression index matches any column it mentions. --#}
  {% set database = adapter.escape_sql_string(relation.database) %}
  {% set schema = adapter.escape_sql_string(relation.schema) %}
  {% set identifier = adapter.escape_sql_string(relation.identifier) %}
  {% call statement('indexes_for_sync_commit_check', fetch_result=True, auto_begin=False) %}
    SELECT expressions
    FROM duckdb_indexes()
    WHERE database_name = '{{ database }}'
      AND schema_name = '{{ schema }}'
      AND table_name = '{{ identifier }}'
  {% endcall %}
  {% set ns = namespace(names_changed=false) %}
  {% for row in load_result('indexes_for_sync_commit_check').table %}
    {% set expressions = (row[0] or '') | lower %}
    {% set words = expressions | replace('[', ' ') | replace(']', ' ') | replace('(', ' ') | replace(')', ' ') | replace("'", ' ') | replace('"', ' ') | replace(',', ' ') | replace('.', ' ') %}
    {% for word in words.split() %}
      {% if word in changed_columns %}
        {% set ns.names_changed = true %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  {{ return(ns.names_changed) }}
{%- endmacro %}

{% macro duckdb__sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) -%}
  {#-- DuckDB cannot alter a table that has an index ("Cannot alter entry ...
       because there are entries that depend on it"), so when a
       sync_all_columns run removes columns or changes types, drop the
       relation's indexes first and recreate them from the model's `indexes`
       config afterwards -- the same rebuild the full-refresh path in
       incremental.sql does, so an index the user removed from the config
       along with its column is not brought back. Only a sync that removes
       or retypes columns touches the indexes; an add-only sync leaves them
       alone, and any other on_schema_change mode is handled by the default
       implementation below.
       DuckDB checks a dropped index against the committed catalog
       (duckdb/duckdb#24086), so DROP INDEX followed by an ALTER on one of
       its columns fails inside one transaction. When an existing index
       names a column being removed or retyped, commit after dropping the
       indexes so the ALTER sees the committed catalog; every other sync
       stays in a single transaction. What a later failure leaves behind
       depends on how the ALTER runs: a column removal runs through
       run_query, which executes with auto_begin=False, so the ALTER and
       the index rebuild commit immediately -- the table is left with the
       synced schema and its indexes, and only the failed run's new rows
       are missing. A type change runs through a statement block, which
       begins a new transaction, so the ALTER and the rebuild roll back
       with the failed transaction and the table is left with its old
       types and no indexes. Either way the next run re-runs the sync and
       restores the table -- see the tests
       test_failing_run_after_sync_restores_indexes and
       test_failing_run_after_type_change_restores_indexes. Once
       duckdb/duckdb#24086 is fixed, the commit (and this comment) can
       come out. --#}
  {%- set remove_from_target_arr = schema_changes_dict['target_not_in_source'] -%}
  {%- set new_target_types = schema_changes_dict['new_target_types'] -%}
  {% if on_schema_change == 'sync_all_columns'
        and (remove_from_target_arr | length > 0 or new_target_types | length > 0) %}
    {%- set changed_columns = [] -%}
    {% for column in remove_from_target_arr %}
      {% do changed_columns.append(column.name | lower) %}
    {% endfor %}
    {% for new_type in new_target_types %}
      {% do changed_columns.append(new_type['column_name'] | lower) %}
    {% endfor %}
    {#-- Check before dropping: duckdb_indexes() only sees the indexes that
         still exist, so the commit decision has to be made first. --#}
    {% set commit_needed = indexes_name_changed_columns(target_relation, changed_columns) %}
    {% do drop_indexes_on_relation(target_relation) %}
    {% if commit_needed and not adapter.disable_transactions() %}
      {% do adapter.commit() %}
    {% endif %}
    {% do default__sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}
    {% do create_indexes(target_relation) %}
  {% else %}
    {% do default__sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}
  {% endif %}
{%- endmacro %}
