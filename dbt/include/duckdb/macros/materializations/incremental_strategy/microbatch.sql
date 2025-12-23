{% macro duckdb__get_incremental_microbatch_sql(arg_dict) -%}
    {# Extract and validate required config #}
    {%- set event_time = config.get('event_time') -%}
    {%- if not event_time -%}
        {{ exceptions.raise_compiler_error("microbatch incremental strategy requires an 'event_time' model config") }}
    {%- endif -%}

    {# Extract batch context - dbt sets these per batch run based on lookback window #}
    {%- set batch_ctx = model.get('batch') -%}
    {%- set batch_start = batch_ctx.get('event_time_start') if batch_ctx else none -%}
    {%- set batch_end = batch_ctx.get('event_time_end') if batch_ctx else none -%}

    {%- if not (batch_start and batch_end) -%}
        {{ exceptions.raise_compiler_error("microbatch incremental strategy requires 'batch.event_time_start' and 'batch.event_time_end' to be set in the context") }}
    {%- endif -%}

    {# Extract remaining arguments #}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = validate_incremental_predicates(arg_dict.get("incremental_predicates")) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {# Build the batch time filter predicate #}
    {%- set batch_predicate -%}
        {{ event_time }} >= cast('{{ batch_start }}' as timestamp)
        and {{ event_time }} < cast('{{ batch_end }}' as timestamp)
    {%- endset -%}

    {# Build combined WHERE clause with optional incremental predicates #}
    {%- set where_clause -%}
        {{ batch_predicate }}
        {%- for predicate in incremental_predicates %}
        and ({{ predicate }})
        {%- endfor %}
    {%- endset -%}

    {# Generate delete + insert SQL #}
    {%- set build_sql -%}
        delete from {{ target }}
        where {{ where_clause }};

        insert into {{ target }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ source }}
        where {{ where_clause }}
    {%- endset -%}

    {{ return(build_sql) }}
{%- endmacro %}
