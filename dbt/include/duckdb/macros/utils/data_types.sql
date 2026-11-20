{#- DuckDB's FLOAT is a 4-byte single-precision type. Postgres, Snowflake and
    BigQuery read a bare `float` as 8-byte double precision, so with the
    duckdb_float_as_double behavior flag a bare `float` becomes DOUBLE here too. -#}

{% macro duckdb__type_float() -%}
  {%- if adapter.behavior.duckdb_float_as_double -%}
    {{ return('DOUBLE') }}
  {%- endif -%}
  {{ return(api.Column.translate_type('float')) }}
{%- endmacro %}

{% macro duckdb__seed_column_type(column_type) -%}
  {#- Only a bare `float` is remapped: FLOAT4/REAL and FLOAT8/DOUBLE say their width,
      and the flag is checked only for a `float` column so others get no warning -#}
  {%- if column_type is string and column_type | trim | lower == 'float'
        and adapter.behavior.duckdb_float_as_double -%}
    {{ return('DOUBLE') }}
  {%- endif -%}
  {{ return(column_type) }}
{%- endmacro %}
