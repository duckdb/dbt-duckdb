{% macro duckdb__get_incremental_microbatch_sql(arg_dict) -%}
    {#--
        Microbatch strategy for time-based batch processing.
        Uses MERGE when available (DuckDB >= 1.4.0) for better performance,
        otherwise falls back to delete+insert.
    --#}

    {%- set target_relation = arg_dict["target_relation"] -%}
    {%- set temp_relation = arg_dict["temp_relation"] -%}
    {%- set unique_key = arg_dict["unique_key"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = arg_dict.get("incremental_predicates") -%}

    {#-- Check if MERGE is available by checking if 'merge' is in valid strategies --#}
    {%- set merge_available = 'merge' in adapter.valid_incremental_strategies() -%}

    {% if merge_available %}
        {#-- Use MERGE for better performance on DuckDB >= 1.4.0 --#}
        {%- set build_sql = duckdb__get_merge_sql(
            target_relation,
            temp_relation,
            unique_key,
            dest_columns,
            incremental_predicates
        ) -%}
    {% else %}
        {#-- Fall back to delete+insert for earlier DuckDB versions --#}
        {%- set build_sql = duckdb__get_delete_insert_merge_sql(
            target_relation,
            temp_relation,
            unique_key,
            dest_columns,
            incremental_predicates
        ) -%}
    {% endif %}

    {{ return(build_sql) }}
{%- endmacro %}
