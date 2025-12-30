{% materialization table, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}
  
  {# Detect S3 Tables for Iceberg-specific SQL patterns #}
  {%- set database = config.get('database') -%}
  {%- set is_s3_tables = false -%}
  {%- if database -%}
    {%- set is_s3_tables = adapter.is_s3_tables_catalog(database) -%}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if is_s3_tables %}
    {# S3 Tables: Use DROP + CREATE pattern (no CREATE OR REPLACE or DROP CASCADE support) #}
    {% if existing_relation is not none %}
      {% call statement('drop_existing_table') %}
        DROP TABLE {{ target_relation }}
      {% endcall %}
      {# Commit the DROP to ensure it's persisted before CREATE #}
      {% if not adapter.disable_transactions() %}
        {% do adapter.commit() %}
      {% endif %}
    {% endif %}
    
    {# Build CREATE TABLE statement #}
    {% call statement('main', language=language) -%}
      CREATE TABLE {{ target_relation }} AS (
        {{ compiled_code }}
      )
    {%- endcall %}
    
    {# Set Iceberg table properties if provided (DuckDB 1.4.2+) #}
    {%- set iceberg_properties = config.get('iceberg_properties', {}) -%}
    {%- if iceberg_properties -%}
      {# Build properties dict for set_iceberg_table_properties #}
      {%- set props_dict = [] -%}
      {%- for key, value in iceberg_properties.items() -%}
        {%- do props_dict.append("'" ~ key ~ "': '" ~ value ~ "'") -%}
      {%- endfor -%}
      {% call statement('set_iceberg_properties') %}
        CALL set_iceberg_table_properties({{ target_relation }}, { {{ props_dict | join(', ') }} })
      {% endcall %}
    {%- endif -%}
    
    {% set need_swap = false %}
  {% else %}
    {# Standard DuckDB: Use intermediate relation and swap #}
    -- build model
    {% call statement('main', language=language) -%}
      {{- create_table_as(False, intermediate_relation, compiled_code, language) }}
    {%- endcall %}

    -- cleanup
    {% if existing_relation is not none %}
        {#-- Drop indexes before renaming to avoid dependency errors --#}
        {% do drop_indexes_on_relation(existing_relation) %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% set need_swap = true %}
  {% endif %}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {% if need_swap %}
    -- finally, drop the existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
