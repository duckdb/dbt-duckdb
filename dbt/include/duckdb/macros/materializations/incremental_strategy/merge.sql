{% macro duckdb__get_incremental_merge_sql(args_dict) %}
  {%- set target_relation = args_dict['target_relation'] -%}
  {%- set temp_relation = args_dict['temp_relation'] -%}
  {%- set unique_key = args_dict['unique_key'] -%}
  {%- set dest_columns = args_dict['dest_columns'] -%}
  {%- set incremental_predicates = args_dict.get('incremental_predicates') -%}

  {%- set build_sql = duckdb__get_merge_sql(target_relation, temp_relation, unique_key, dest_columns, incremental_predicates) -%}

  {{ return(build_sql) }}
{% endmacro %}

{% macro duckdb__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    
    {#-- DuckDB specific configurations --#}
    {%- set merge_update_by_name = config.get('merge_update_by_name', false) -%}
    {%- set merge_insert_by_name = config.get('merge_insert_by_name', false) -%}
    {%- set merge_update_all = config.get('merge_update_all', false) -%}
    {%- set merge_insert_all = config.get('merge_insert_all', false) -%}
    {%- set when_not_matched_by_source = config.get('when_not_matched_by_source') -%}
    {%- set when_not_matched_by_target = config.get('when_not_matched_by_target') -%}
    {%- set merge_error_on_matched = config.get('merge_error_on_matched') -%}
    {%- set merge_error_on_not_matched = config.get('merge_error_on_not_matched') -%}
    {%- set merge_error_on_not_matched_by_source = config.get('merge_error_on_not_matched_by_source') -%}
    {%- set merge_matched_action = config.get('merge_matched_action', 'update') -%}
    {%- set merge_not_matched_action = config.get('merge_not_matched_action', 'insert') -%}
    {%- set merge_use_using_clause = config.get('merge_use_using_clause', false) -%}
    {%- set merge_using_columns = config.get('merge_using_columns') -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set source_unique_key = ("DBT_INTERNAL_SOURCE." ~ unique_key) | trim %}
            {% set target_unique_key = ("DBT_INTERNAL_DEST." ~ unique_key) | trim %}
            {% set unique_key_match = equals(source_unique_key, target_unique_key) | trim %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        {%- if merge_use_using_clause and merge_using_columns %}
        using {{ source }} as DBT_INTERNAL_SOURCE
        using ({{ merge_using_columns | join(', ') }})
        {%- else %}
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}
        {%- endif %}

    {% if unique_key %}
        {%- if merge_error_on_matched %}
    when matched {% if merge_error_on_matched.condition %}and {{ merge_error_on_matched.condition }}{% endif %} then error {% if merge_error_on_matched.message %}{{ merge_error_on_matched.message }}{% endif %}
        {%- endif %}
        
        {%- if merge_matched_action == 'update' %}
    when matched then 
            {%- if merge_update_all %}
        update set *
            {%- elif merge_update_by_name %}
        update by name
            {%- else %}
        update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
            {%- endif %}
        {%- elif merge_matched_action == 'delete' %}
    when matched then delete
        {%- elif merge_matched_action == 'do_nothing' %}
    when matched then do nothing
        {%- endif %}
    {% endif %}

    {% if when_not_matched_by_source %}
        {%- if merge_error_on_not_matched_by_source %}
    when not matched by source {% if merge_error_on_not_matched_by_source.condition %}and {{ merge_error_on_not_matched_by_source.condition }}{% endif %} then error {% if merge_error_on_not_matched_by_source.message %}{{ merge_error_on_not_matched_by_source.message }}{% endif %}
        {%- endif %}
        
        {%- if when_not_matched_by_source == 'delete' %}
    when not matched by source then delete
        {%- elif when_not_matched_by_source == 'update' %}
            {%- if merge_update_all %}
    when not matched by source then update set *
            {%- elif merge_update_by_name %}
    when not matched by source then update by name
            {%- elif when_not_matched_by_source is mapping and when_not_matched_by_source.update_columns %}
    when not matched by source then update set
        {% for column_name in when_not_matched_by_source.update_columns -%}
            {{ column_name }} = {{ when_not_matched_by_source.update_values[column_name] | default('NULL') }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
            {%- else %}
    when not matched by source then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = NULL
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
            {%- endif %}
        {%- elif when_not_matched_by_source == 'do_nothing' %}
    when not matched by source then do nothing
        {%- endif %}
    {% endif %}

    {%- if merge_error_on_not_matched %}
    when not matched {% if merge_error_on_not_matched.condition %}and {{ merge_error_on_not_matched.condition }}{% endif %} then error {% if merge_error_on_not_matched.message %}{{ merge_error_on_not_matched.message }}{% endif %}
    {%- endif %}
    
    {%- if merge_not_matched_action == 'insert' %}
        {%- if when_not_matched_by_target == false %}
        {%- elif merge_insert_all %}
    when not matched then insert *
        {%- elif merge_insert_by_name %}
    when not matched then insert by name
        {%- else %}
    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({% for column in dest_columns %}DBT_INTERNAL_SOURCE.{{ column.name | as_text }}{% if not loop.last %}, {% endif %}{% endfor %})
        {%- endif %}
    {%- elif merge_not_matched_action == 'do_nothing' %}
    when not matched then do nothing
    {%- endif %}

{% endmacro %}



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
