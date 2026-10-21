{% materialization table, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}

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
  {%- set use_ducklake_table_workarounds = adapter.use_ducklake_table_workarounds(target_relation) -%}
  {%- set build_in_place = has_foreign_key_constraints(existing_relation) -%}
  {%- set dropped_dependents = [] -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {%- set partitioned_by = duckdb__get_partitioned_by(target_relation, false) -%}
  {%- set sorted_by = duckdb__get_sorted_by(target_relation, false) -%}

  {% if build_in_place %}
    {#-- Tables with foreign keys must not be renamed (see has_foreign_key_constraints),
         so replace the existing relation inside this transaction instead of swapping --#}
    {% if existing_relation is not none %}
      {% set dropped_dependents = drop_relation_for_rebuild(existing_relation) %}
    {% endif %}
    {% call statement('main', language=language) -%}
      {{- create_table_as(False, target_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) }}
    {%- endcall %}
    {% do restore_foreign_key_dependents(dropped_dependents) %}
  {% else %}
    -- build model
    {% call statement('main', language=language) -%}
      {{- create_table_as(False, intermediate_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) }}
    {%- endcall %}

    -- cleanup
    {% if existing_relation is not none %}
        {#-- Drop indexes and foreign key dependents before renaming to avoid dependency errors --#}
        {% do drop_indexes_on_relation(existing_relation) %}
        {% if existing_relation.is_table and may_be_foreign_key_target(existing_relation) %}
          {% set dropped_dependents = drop_foreign_key_dependents(existing_relation) %}
        {% endif %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% do restore_foreign_key_dependents(dropped_dependents) %}
  {% endif %}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% if not use_ducklake_table_workarounds %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {% if use_ducklake_table_workarounds %}
    {# Before DuckDB 1.5.3, DuckLake could lose table ALTER metadata during rename. #}
    {% if partitioned_by %}
      {% call statement('reassert_partitioned_by', auto_begin=False) -%}
        {{ duckdb__alter_table_set_partitioned_by(target_relation, partitioned_by) }}
      {%- endcall %}
    {% endif %}
    {% if sorted_by %}
      {% call statement('reassert_sorted_by', auto_begin=False) -%}
        {{ duckdb__alter_table_set_sorted_by(target_relation, sorted_by) }}
      {%- endcall %}
    {% endif %}
    {# Pre-1.5.3 DuckLake versions can conflict when comments are persisted in the transaction. #}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
