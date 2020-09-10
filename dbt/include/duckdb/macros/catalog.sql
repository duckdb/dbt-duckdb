
{% macro duckdb__get_catalog(information_schema, schemas) -%}

  {% set msg -%}
    get_catalog not implemented for duckdb
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}
