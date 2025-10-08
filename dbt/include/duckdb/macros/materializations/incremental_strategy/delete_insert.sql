{% macro duckdb__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target }} as DBT_INCREMENTAL_TARGET
            using {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = DBT_INCREMENTAL_TARGET.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}


{% macro duckdb__get_incremental_microbatch_sql(arg_dict) -%}
    {#-- Microbatch strategy uses delete+insert for DuckDB --#}
    {#-- This works efficiently for time-based batch processing --#}
    {% do return(duckdb__get_delete_insert_merge_sql(
        arg_dict["target_relation"],
        arg_dict["temp_relation"],
        arg_dict["unique_key"],
        arg_dict["dest_columns"],
        arg_dict["incremental_predicates"]
    )) %}
{%- endmacro %}
