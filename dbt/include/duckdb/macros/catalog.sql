
{% macro duckdb__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
        '{{ database }}' as table_database,
        t.table_schema,
        t.table_name,
        t.table_type,
        '' as table_comment,
        c.column_name,
        c.ordinal_position as column_index,
        c.data_type column_type,
        '' as column_comment,
        '' as table_owner
    FROM information_schema.tables t JOIN information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name
    WHERE (
        {%- for schema in schemas -%}
          upper(t.table_schema) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
    AND t.table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY
        t.table_schema,
        t.table_name,
        c.ordinal_position
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
