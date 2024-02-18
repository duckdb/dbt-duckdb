{% macro duckdb__datediff(first_date, second_date, datepart) -%}
    date_diff('{{ datepart }}', {{ first_date }}::timestamp, {{ second_date}}::timestamp )
{%- endmacro %}
