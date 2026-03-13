{% macro duckdb__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {% set sql %}
        select type from duckdb_databases()
        where lower(database_name)='{{ relation.database | lower }}'
        and type='sqlite'
    {% endset %}
    {% set results = run_query(sql) %}
    {% if results|length == 0 %}
        create schema if not exists {{ relation.without_identifier() }}
    {% else %}
        {% if relation.schema!='main' %}
            {{ exceptions.raise_compiler_error(
                "Schema must be 'main' when writing to sqlite "
                ~ "instead got " ~ relation.schema
            )}}
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
      {{ col['name'] }} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}


{% macro duckdb__is_valid_partition_identifier(identifier) -%}
  {%- set ident = identifier | trim -%}
  {%- if ident == '' -%}
    {{ return(false) }}
  {%- endif -%}

  {# Allow quoted identifiers, but disallow embedded quotes. #}
  {%- if ident[:1] == '"' and ident[-1:] == '"' and ident | length > 2 -%}
    {%- set inner = ident[1:-1] -%}
    {%- if '"' in inner -%}
      {{ return(false) }}
    {%- endif -%}
    {{ return(true) }}
  {%- endif -%}

  {%- set first_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_" -%}
  {%- set rest_chars = first_chars ~ "0123456789" -%}

  {%- if ident[0] not in first_chars -%}
    {{ return(false) }}
  {%- endif -%}

  {%- for ch in ident[1:] -%}
    {%- if ch not in rest_chars -%}
      {{ return(false) }}
    {%- endif -%}
  {%- endfor -%}

  {{ return(true) }}
{%- endmacro %}


{% macro duckdb__get_partitioned_by(relation, temporary) -%}
  {%- if temporary -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set raw = config.get('partitioned_by', none) -%}
  {%- if raw is none -%}
    {%- set raw = config.get('partition_by', none) -%}
  {%- endif -%}
  {%- if raw is none -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set type_error = "partitioned_by/partition_by must be a string or list of strings" -%}
  {%- set list_item_error = "partitioned_by/partition_by list values must be non-empty strings" -%}
  {%- set value_error = "partitioned_by/partition_by values must be valid column identifiers" -%}
  {%- set empty_list_error = "partitioned_by/partition_by list must contain at least one value" -%}

  {%- if raw is mapping -%}
    {% do exceptions.raise_compiler_error(type_error) %}
  {%- elif raw is string -%}
    {%- set source_is_list = false -%}
    {%- set value = raw | trim -%}
    {%- if value == '' -%}
      {{ return(none) }}
    {%- endif -%}
    {%- if value[:1] == '(' and value[-1:] == ')' -%}
      {%- set value = value[1:-1] | trim -%}
    {%- endif -%}
    {%- set raw_parts = value.split(',') -%}
  {%- elif raw is iterable -%}
    {%- set source_is_list = true -%}
    {%- set raw_parts = raw -%}
    {%- if raw_parts | length == 0 -%}
      {% do exceptions.raise_compiler_error(empty_list_error) %}
    {%- endif -%}
  {%- else -%}
    {% do exceptions.raise_compiler_error(type_error) %}
  {%- endif -%}

  {%- set value_parts = [] -%}
  {%- for part in raw_parts -%}
    {%- if part is not string -%}
      {% do exceptions.raise_compiler_error(list_item_error) %}
    {%- endif -%}
    {%- set cleaned_part = part | trim -%}
    {%- if cleaned_part == '' -%}
      {% if source_is_list %}
        {% do exceptions.raise_compiler_error(list_item_error) %}
      {% else %}
        {% do exceptions.raise_compiler_error(value_error) %}
      {% endif %}
    {%- endif -%}
    {%- if not duckdb__is_valid_partition_identifier(cleaned_part) -%}
      {% do exceptions.raise_compiler_error(value_error) %}
    {%- endif -%}
    {%- do value_parts.append(cleaned_part) -%}
  {%- endfor -%}
  {%- set value = value_parts | join(', ') -%}

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

  {{ return(value) }}
{%- endmacro %}


{% macro duckdb__alter_table_set_partitioned_by(relation, partitioned_by) -%}
  alter table {{ relation }} set partitioned by ({{ partitioned_by }});
{%- endmacro %}

{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none) -%}
  {%- if language == 'sql' -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(compiled_code) }}
    {% endif %}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {% if contract_config.enforced and not temporary %}
    {#-- DuckDB doesnt support constraints on temp tables --#}
    {{ get_table_columns_and_constraints() }} ;
    {% if partitioned_by %}
      {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
    {% endif %}
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% else %}
    {% if partitioned_by %}
    as (
      select * from (
        {{ compiled_code }}
      ) as model_subq
      limit 0
    );
    {{ duckdb__alter_table_set_partitioned_by(relation, partitioned_by) }}
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
    {{ py_write_table(temporary=temporary, relation=relation, compiled_code=compiled_code, partitioned_by=partitioned_by) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("duckdb__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}
{% endmacro %}

{% macro py_write_table(temporary, relation, compiled_code, partitioned_by=none) -%}
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
    {% if partitioned_by %}
    con.execute('create table {{ relation }} as select * from ' + tmp_name + ' limit 0')
    con.execute('alter table {{ relation }} set partitioned by ({{ partitioned_by }})')
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

{% macro duckdb__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from system.information_schema.columns
      where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and lower(table_schema) = '{{ relation.schema | lower }}'
      {% endif %}
      {% if relation.database %}
      and lower(table_catalog) = '{{ relation.database | lower }}'
      {% endif %}
      order by ordinal_position

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
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
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
  {% call statement('get_indexes_on_relation', fetch_result=True) %}
    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
  {% endcall %}

  {% set results = load_result('get_indexes_on_relation').table %}
  {% for row in results %}
    {% set index_name = row[0] %}
    {% call statement('drop_index_' + loop.index|string, auto_begin=false) %}
      DROP INDEX "{{ relation.schema }}"."{{ index_name }}"
    {% endcall %}
  {% endfor %}

  {#-- Verify indexes were dropped --#}
  {% call statement('verify_indexes_dropped', fetch_result=True) %}
    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
  {% endcall %}
  {% set verify_results = load_result('verify_indexes_dropped').table %}
{%- endmacro %}
