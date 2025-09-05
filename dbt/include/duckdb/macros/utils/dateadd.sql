{% macro duckdb__dateadd(datepart, interval, from_date_or_timestamp) %}

    -- Use INTERVAL without parentheses for compatibility with newer DuckDB versions
    date_add({{ from_date_or_timestamp }}, interval {{ interval }} {{ datepart }})

{% endmacro %}
