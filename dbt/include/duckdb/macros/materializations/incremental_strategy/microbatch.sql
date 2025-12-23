{% macro duckdb__get_incremental_microbatch_sql(arg_dict) -%}
    {#--
        Microbatch strategy for time-based batch processing.
        Uses MERGE when available (DuckDB >= 1.4.0) for better performance,
        otherwise falls back to delete+insert.
    --#}

    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = validate_incremental_predicates(arg_dict.get("incremental_predicates")) -%}
    {%- set event_time = config.get('event_time') -%}
    {%- if not event_time -%}
        {{ exceptions.raise_compiler_error("microbatch incremental strategy requires an 'event_time' model config") }}
    {%- endif -%}

    {# dbt sets node.batch.event_time_start/end per batch run depending on lookback window #}
    {%- set batch_ctx = model.get('batch') -%}
    {%- if batch_ctx -%}
        {%- set batch_start = batch_ctx.get('event_time_start') -%}
        {%- set batch_end = batch_ctx.get('event_time_end') -%}
    {%- endif -%}

    {%- if batch_start and batch_end -%}
        {%- set batch_start_str = batch_start | string -%}
        {%- set batch_end_str = batch_end | string -%}
        {%- set batch_predicate = 
            event_time
            ~ " >= cast('" ~ batch_start_str ~ "' as timestamp)"
            ~ " and "
            ~ event_time
            ~ " < cast('" ~ batch_end_str ~ "' as timestamp)"
         -%}

        {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) %}
        
        {%- set build_sql -%}
            delete from {{ target }}
            where {{ batch_predicate}}
            {%- for predicate in incremental_predicates %}
                and ({{ predicate }})
            {%- endfor %};

            insert into {{ target }} ({{ dest_cols_csv }})
            (
                select {{ dest_cols_csv }}
                from {{ source }}
                where {{ batch_predicate }}
                {%- for predicate in incremental_predicates %}
                    and ({{ predicate }})
                {%- endfor %}
            )   
        {%- endset -%}    

    {%- else -%}
        {{ exceptions.raise_compiler_error("microbatch incremental strategy requires 'batch.event_time_start' and 'batch.event_time_end' to be set in the context") }}
    {%- endif -%}
    
    {{ return(build_sql) }}
{%- endmacro %}
