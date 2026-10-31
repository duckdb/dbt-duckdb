{% macro duckdb__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {% set sql %}
        select type from duckdb_databases()
        where lower(database_name)='{{ relation.database | lower }}'
        and type != 'duckdb'
    {% endset %}
    {% set results = run_query(sql) %}
    {% if results|length == 0 %}
        create schema if not exists {{ relation.without_identifier() }}
    {% else %}
        {% set attached_type = results.columns[0].values()[0] %}
        {% if attached_type == 'sqlite' and relation.schema != 'main' %}
            {{ exceptions.raise_compiler_error(
                "Schema must be 'main' when writing to sqlite "
                ~ "instead got " ~ relation.schema
            )}}
        {% elif attached_type not in ['sqlite', 'mysql'] %}
            create schema if not exists {{ relation.without_identifier() }}
        {% endif %}
    {% endif %}
  {%- endcall -%}
{% endmacro %}

{% macro duckdb__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro duckdb__list_schemas(database) -%}
  {% set sql %}
    select schema_name
    from system.information_schema.schemata
    {% if database is not none %}
    where lower(catalog_name) = '{{ database | lower }}'
    {% endif %}
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro duckdb__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from system.information_schema.schemata
        where lower(schema_name) = '{{ schema | lower }}'
        and lower(catalog_name) = '{{ information_schema.database | lower }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro get_column_names() %}
  {# loop through user_provided_columns to get column names #}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
      {{ col_name }} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}


{% macro duckdb__get_partitioned_by(relation, temporary) -%}
  {%- if temporary -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set raw = config.get('partitioned_by', none) -%}
  {%- if raw is none or raw == '' -%}
    {%- set raw = config.get('partition_by', none) -%}
  {%- endif -%}
  {%- if raw is none or raw == '' -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set parts = normalize_string_or_list(raw, "partitioned_by/partition_by") -%}

  {%- if parts | length == 0 -%}
    {% do exceptions.raise_compiler_error("partitioned_by/partition_by must contain at least one column") %}
  {%- endif -%}

  {# Apply partitioning only on the final target relation, not staging/intermediate relations. #}
  {%- if this is defined and config.get('materialized') in ['table', 'incremental']
      and relation.identifier and this.identifier and relation.identifier != this.identifier -%}
    {{ return(none) }}
  {%- endif -%}

  {%- if not adapter.is_ducklake(relation) -%}
    {% do adapter.warn_once(
      "partitioned_by/partition_by is only supported for DuckLake relations; ignoring for "
      ~ relation
    ) %}
    {{ return(none) }}
  {%- endif -%}

  {{ return(parts | join(', ')) }}
{%- endmacro %}


{% macro duckdb__alter_table_set_partitioned_by(relation, partitioned_by) -%}
  alter table {{ relation }} set partitioned by ({{ partitioned_by }});
{%- endmacro %}


{% macro duckdb__get_sorted_by(relation, temporary) -%}
  {%- if temporary -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set raw = config.get('sorted_by', none) -%}
  {%- if raw is none -%}
    {%- set raw = config.get('sort_by', none) -%}
  {%- endif -%}
  {%- if raw is none -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set parts = normalize_string_or_list(raw, "sorted_by/sort_by") -%}

  {%- if parts | length == 0 -%}
    {% do exceptions.raise_compiler_error("sorted_by/sort_by must contain at least one column") %}
  {%- endif -%}

  {# Apply sorting only on the final target relation, not staging/intermediate relations. #}
  {%- if this is defined and config.get('materialized') in ['table', 'incremental']
      and relation.identifier and this.identifier and relation.identifier != this.identifier -%}
    {{ return(none) }}
  {%- endif -%}

  {%- if not adapter.is_ducklake(relation) -%}
    {% do adapter.warn_once(
      "sorted_by/sort_by is only supported for DuckLake relations; ignoring for "
      ~ relation
    ) %}
    {{ return(none) }}
  {%- endif -%}

  {%- set quoted = [] -%}
  {%- for col in parts -%}
    {%- set escaped = col | replace('"', '""') -%}
    {%- do quoted.append(adapter.quote(escaped)) -%}
  {%- endfor -%}
  {{ return(quoted | join(', ')) }}
{%- endmacro %}


{% macro duckdb__alter_table_set_sorted_by(relation, sorted_by) -%}
  alter table {{ relation }} set sorted by ({{ sorted_by }});
{%- endmacro %}

{% macro create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {{ return(adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by)) }}
{%- endmacro %}

{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {%- if language == 'sql' -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(compiled_code) }}
    {% endif %}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

  {% if contract_config.enforced and not temporary and relation.database %}
    {#-- DuckDB rejects catalog-qualified foreign key targets, so bind
         unqualified/schema-qualified targets in the table's catalog. --#}
    {% do run_query('use ' ~ adapter.quote(relation.database)) %}
  {% endif %}

    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {% if contract_config.enforced and not temporary %}
    {#-- DuckDB doesnt support constraints on temp tables --#}
    {{ get_table_columns_and_constraints() }} ;
    {% if partitioned_by %}
      {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
    {% endif %}
    {% if sorted_by %}
      {{ duckdb__alter_table_set_sorted_by(relation, sorted_by) }}
    {% endif %}
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% else %}
    {% if partitioned_by or sorted_by %}
    as (
      select * from (
        {{ compiled_code }}
      ) as model_subq
      limit 0
    );
    {% if partitioned_by %}
    {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
    {% endif %}
    {% if sorted_by %}
    {{ duckdb__alter_table_set_sorted_by(relation, sorted_by) }}
    {% endif %}
    insert into {{ relation }}
      select * from (
        {{ compiled_code }}
      ) as model_subq;
    {% else %}
    as (
      {{ compiled_code }}
    );
    {% endif %}
  {% endif %}
  {%- elif language == 'python' -%}
    {{ py_write_table(temporary=temporary, relation=relation, compiled_code=compiled_code, partitioned_by=partitioned_by, sorted_by=sorted_by) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("duckdb__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}
{% endmacro %}

{% macro py_write_table(temporary, relation, compiled_code, partitioned_by=none, sorted_by=none) -%}
{{ compiled_code }}

def materialize(df, con):
    try:
        import pyarrow
        pyarrow_available = True
    except ImportError:
        pyarrow_available = False
    finally:
        if pyarrow_available and isinstance(df, pyarrow.Table):
            # https://github.com/duckdb/duckdb/issues/6584
            import pyarrow.dataset
    tmp_name = '__dbt_python_model_df_' + '{{ relation.identifier }}'
    con.register(tmp_name, df)
    {% if partitioned_by or sorted_by %}
    con.execute('create table {{ relation }} as select * from ' + tmp_name + ' limit 0')
    {% if partitioned_by %}
    con.execute('alter table {{ relation }} set partitioned by ({{ partitioned_by }})')
    {% endif %}
    {% if sorted_by %}
    con.execute('alter table {{ relation }} set sorted by ({{ sorted_by }})')
    {% endif %}
    con.execute('insert into {{ relation }} select * from ' + tmp_name)
    {% else %}
    con.execute('create table {{ relation }} as select * from ' + tmp_name)
    {% endif %}
    con.unregister(tmp_name)
{% endmacro %}

{% macro duckdb__create_view_as(relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro duckdb__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {#- get_column_schema_from_query wraps this in DESCRIBE (...), which cannot parse
        an inlined sql_header (issue #515), so run the header separately to keep its
        session effects visible to the describe -#}
    {%- if select_sql_header is not none -%}
      {%- do run_query(select_sql_header) -%}
    {%- endif -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    where false
    limit 0
{% endmacro %}

{% macro duckdb__get_columns_in_relation(relation) -%}
  {# DESCRIBE avoids the global information_schema.columns scan, which forces a full
     catalog load on attached catalogs like DuckLake (issue #762). DuckDB's column_type
     carries precision/scale in the type string, so the null literals are safe. #}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          column_type as data_type,
          cast(null as bigint) as character_maximum_length,
          cast(null as bigint) as numeric_precision,
          cast(null as bigint) as numeric_scale
      from (describe {{ relation.render() }})
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro duckdb__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      table_name as name,
      table_schema as schema,
      CASE table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        WHEN 'LOCAL TEMPORARY' THEN 'table'
        END as type
    from system.information_schema.tables
    where lower(table_schema) = '{{ schema_relation.schema | lower }}'
    and lower(table_catalog) = '{{ schema_relation.database | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro duckdb__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if adapter.is_ducklake(relation) %}
      drop {{ relation.type }} if exists {{ relation }}
    {% else %}
      drop {{ relation.type }} if exists {{ relation }} cascade
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro duckdb__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter {{ to_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{% macro duckdb__make_temp_relation(base_relation, suffix) %}
    {# Creates duckdb temporary tables, i.e. ephemeral per connection and on the temp.main schema.
    Temporary tables are always allocated in the DuckdDB client's memory, even when called from a MotherDuck connection. #}

    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
{% endmacro %}

{% macro duckdb__dispatch_temporary_relation(target_relation, batch_id='') %}
    {# Prepares a relation that is meant to be temporary.

     This macro is called when creating the temporary relations needed for incremental models and snapshots.

     When running against duckdb,
     it dispatches to standard dbt-core machinery to create a temporary table.
     However, MotherDuck doesn't support server-side temporary tables,
     so it dispatches to a fully qualified unique table relation under a temporary schema. #}

    {% if adapter.is_motherduck() %}
        {% set relation = target_relation.incorporate(
            path=adapter.get_temp_relation_path(target_relation, batch_id)
        ) %}
        {% do run_query(create_schema(relation)) %}
        {% if not adapter.disable_transactions() %}
            {% do adapter.commit() %}
        {% endif %}
        {% do return({'relation': relation, 'temporary': false}) %}
    {% else %}
        {% do return({
            'relation': make_temp_relation(target_relation),
            'temporary': true
        }) %}
    {% endif %}
{% endmacro %}

{% macro duckdb__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro duckdb__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro duckdb__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp
{%- endmacro %}

{% macro duckdb__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}

{% macro location_exists(location) -%}
  {% do return(adapter.location_exists(location)) %}
{% endmacro %}

{% macro write_to_file(relation, location, options) -%}
  {% call statement('write_to_file') -%}
    copy {{ relation }} to '{{ location }}' ({{ options }})
  {%- endcall %}
{% endmacro %}

{% macro store_relation(plugin, relation, location, format, config) -%}
  {%- set column_list = adapter.get_columns_in_relation(relation) -%}
  {% do adapter.store_relation(plugin, relation, column_list, location, format, config) %}
{% endmacro %}

{% macro render_write_options(config) -%}
  {% set options = config.get('options', {}) %}
  {% if options is not mapping %}
    {% do exceptions.raise_compiler_error("The options argument must be a dictionary") %}
  {% endif %}

  {% for k in options %}
    {% set _ = options.update({k: render(options[k])}) %}
  {% endfor %}

  {# legacy top-level write options #}
  {% if config.get('format') %}
    {% set _ = options.update({'format': render(config.get('format'))}) %}
  {% endif %}
  {% if config.get('delimiter') %}
    {% set _ = options.update({'delimiter': render(config.get('delimiter'))}) %}
  {% endif %}

  {% do return(options) %}
{%- endmacro %}

{% macro duckdb__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}
      {{ adapter.warn_once('Grants for relations are not supported by DuckDB') }}
    {% endif %}
{% endmacro %}

{% macro duckdb__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}

  create {% if index_config.unique -%}
    unique
  {%- endif %} index
  "{{ index_name }}"
  on {{ relation }}
  ({{ comma_separated_columns }});
{%- endmacro %}

{% macro drop_indexes_on_relation(relation) -%}
  {#-- auto_begin=False on these reads so this macro can run safely after
       adapter.commit() in a materialization cleanup without opening a new
       transaction that would later get rolled back at connection release. #}
  {% call statement('get_indexes_on_relation', fetch_result=True, auto_begin=False) %}
    SELECT index_name
    FROM duckdb_indexes()
    WHERE database_name = '{{ relation.database }}'
      AND schema_name = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
  {% endcall %}

  {% set results = load_result('get_indexes_on_relation').table %}
  {% for row in results %}
    {% set index_name = row[0] %}
    {% call statement('drop_index_' + loop.index|string, auto_begin=false) %}
      DROP INDEX "{{ relation.database }}"."{{ relation.schema }}"."{{ index_name }}"
    {% endcall %}
  {% endfor %}
{%- endmacro %}

{% macro drop_foreign_key_dependents(relation) -%}
  {#-- DuckDB will not rename or drop a table that a foreign key references, and it
       cannot drop a foreign key constraint, so the tables whose foreign keys point at
       `relation` (directly or transitively) have to be dropped before it is swapped.
       We only drop tables this run is about to rebuild from scratch anyway: selected
       table models, or incremental models being fully refreshed, that are downstream
       of the model being built. Anything else is an error, so no data is lost.
       Foreign keys cannot cross schemas in DuckDB, so we only look in this one. #}
  {% set database = adapter.escape_sql_string(relation.database) %}
  {% set schema = adapter.escape_sql_string(relation.schema) %}
  {% set identifier = adapter.escape_sql_string(relation.identifier) %}
  {% call statement('get_foreign_key_dependents', fetch_result=True) %}
    with recursive fks as (
      select table_name, referenced_table
      from duckdb_constraints()
      where constraint_type = 'FOREIGN KEY'
        and lower(database_name) = lower('{{ database }}')
        and lower(schema_name) = lower('{{ schema }}')
        and lower(table_name) <> lower(referenced_table)
    ),
    dependents(table_name, depth) as (
      select table_name, 1
      from fks
      where lower(referenced_table) = lower('{{ identifier }}')
      union
      select fks.table_name, dependents.depth + 1
      from fks
      join dependents on lower(fks.referenced_table) = lower(dependents.table_name)
    )
    {#-- One row per (dependent, table it references), deepest dependents first,
         which is the order they have to be dropped in --#}
    select d.table_name, d.depth, f.referenced_table
    from (select table_name, max(depth) as depth from dependents group by table_name) d
    join fks f on f.table_name = d.table_name
    order by d.depth desc, d.table_name, f.referenced_table
  {% endcall %}
  {% set results = load_result('get_foreign_key_dependents').table %}
  {% if results | length == 0 %}
    {{ return([]) }}
  {% endif %}

  {% set dependents = [] %}
  {% set referenced_by_table = {} %}
  {% for row in results %}
    {% if row[0] not in dependents %}
      {% do dependents.append(row[0]) %}
      {% do referenced_by_table.update({row[0]: []}) %}
    {% endif %}
    {% do referenced_by_table[row[0]].append(row[2] | lower) %}
  {% endfor %}

  {#-- Map each table in the chain back to the model that builds it --#}
  {% set chain = {relation.identifier | lower: model.unique_id} %}
  {% set nodes_by_table = {} %}
  {% for node in graph.nodes.values() if node.resource_type == 'model'
        and (node.database or '') | lower == (relation.database or '') | lower
        and node.schema | lower == relation.schema | lower %}
    {% for table in dependents if (node.alias or node.name) | lower == table | lower %}
      {% do nodes_by_table.update({table: node}) %}
      {% do chain.update({table | lower: node.unique_id}) %}
    {% endfor %}
  {% endfor %}

  {% set problems = [] %}
  {% for table in dependents %}
    {% set node = nodes_by_table.get(table) %}
    {#-- The model must ref() at least one table in the chain, so dbt rebuilds it after this one --#}
    {% set upstream_ids = [] %}
    {% for referenced in referenced_by_table[table] if referenced in chain %}
      {% do upstream_ids.append(chain[referenced]) %}
    {% endfor %}
    {% if node is none %}
      {% do problems.append(table ~ " (not built by a dbt model in this project)") %}
    {% elif node.unique_id not in selected_resources %}
      {% do problems.append(table ~ " (model " ~ node.name ~ " is not selected in this run)") %}
    {% elif upstream_ids | select('in', node.depends_on.nodes) | list | length == 0 %}
      {% do problems.append(table ~ " (model " ~ node.name ~ " does not ref() the table its foreign key points at, so dbt may not rebuild it afterwards; use `to: ref(...)` in the constraint)") %}
    {% else %}
      {% set full_refresh = node.config.full_refresh if node.config.full_refresh is not none else flags.FULL_REFRESH %}
      {% if not (node.config.materialized == 'table' or (node.config.materialized == 'incremental' and full_refresh)) %}
        {% do problems.append(table ~ " (model " ~ node.name ~ " is materialized as " ~ node.config.materialized ~ " and would lose its data; run with --full-refresh)") %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% if problems %}
    {% do exceptions.raise_compiler_error(
      "Cannot replace " ~ relation ~ " because DuckDB does not allow replacing a table that a foreign key "
      ~ "references, and dbt-duckdb can only drop referencing tables that this run will rebuild. "
      ~ "Problem tables: " ~ problems | join("; ")) %}
  {% endif %}

  {% set dropped = [] %}
  {% for table in dependents %}
    {% set dependent = api.Relation.create(database=relation.database, schema=relation.schema, identifier=table, type='table') %}
    {% set backup = dependent.incorporate(path={'identifier': table ~ '__dbt_fk_backup'}, type='table') %}
    {% do adapter.drop_relation(backup) %}
    {% call statement('backup_foreign_key_dependent_' ~ loop.index) %}
      create table {{ backup }} as select * from {{ dependent }}
    {% endcall %}
    {{ log("Dropping " ~ dependent ~ " because it references " ~ relation ~ " through a foreign key; it is rebuilt later in this run", info=True) }}
    {% do adapter.drop_relation(dependent) %}
    {% do dropped.append({'relation': dependent, 'backup': backup}) %}
  {% endfor %}
  {{ return(dropped) }}
{%- endmacro %}

{% macro restore_foreign_key_dependents(dropped) -%}
  {#-- Keep dependent data available between model transactions. The selected model
       rebuilds the constraints later in the run. #}
  {% for item in dropped | reverse %}
    {{ log("Restoring data for " ~ item['relation'] ~ "; its constraints are rebuilt later in this run", info=True) }}
    {% do adapter.rename_relation(item['backup'], item['relation']) %}
  {% endfor %}
{%- endmacro %}

{% macro enforced_constraint_types() -%}
  {#-- The constraint types declared by this model's enforced contract --#}
  {% set contract_config = config.get('contract') %}
  {% set types = [] %}
  {% if contract_config and contract_config.enforced %}
    {% for constraint in model.get('constraints', []) %}
      {% do types.append(constraint.type) %}
    {% endfor %}
    {% for column in model.get('columns', {}).values() %}
      {% for constraint in column.get('constraints', []) %}
        {% do types.append(constraint.type) %}
      {% endfor %}
    {% endfor %}
  {% endif %}
  {{ return(types) }}
{%- endmacro %}

{% macro existing_constraint_types(relation) -%}
  {% if relation is none or not relation.is_table %}
    {{ return([]) }}
  {% endif %}
  {{ return(adapter.existing_key_constraint_types(relation)) }}
{%- endmacro %}

{% macro has_foreign_key_constraints(relation=none) -%}
  {#-- DuckDB records the name a foreign key table was created under on the table it
       references and does not update it on rename (duckdb/duckdb#24648), after which
       the referenced table can never be dropped. Tables with foreign keys therefore
       have to be created under their final name rather than renamed into place. #}
  {% if not adapter.catalog_supports_foreign_keys(this.database) %}
    {{ return(false) }}
  {% endif %}
  {{ return(
    'foreign_key' in enforced_constraint_types()
    or 'foreign_key' in existing_constraint_types(relation)
  ) }}
{%- endmacro %}

{% macro may_be_foreign_key_target(relation=none) -%}
  {#-- Only a table with a primary key or unique constraint can be referenced by a
       foreign key. Include the existing table so removing a key constraint still
       handles its current dependents safely. #}
  {% if not adapter.catalog_supports_foreign_keys(this.database) %}
    {{ return(false) }}
  {% endif %}
  {% set types = enforced_constraint_types() + existing_constraint_types(relation) %}
  {{ return('primary_key' in types or 'unique' in types) }}
{%- endmacro %}

{% macro drop_relation_for_rebuild(relation) -%}
  {#-- Drop a relation inside the model's transaction so it can be rebuilt under the
       same name, dropping anything whose foreign key references it first. The
       transaction is required even with disable_transactions so a failed build rolls
       back the target table and its dependent backups together. --#}
  {% do adapter.ensure_transaction() %}
  {% set dropped = [] %}
  {% if relation.is_table and may_be_foreign_key_target(relation) %}
    {% set dropped = drop_foreign_key_dependents(relation) %}
  {% endif %}
  {% do adapter.drop_relation(relation) %}
  {{ return(dropped) }}
{%- endmacro %}
