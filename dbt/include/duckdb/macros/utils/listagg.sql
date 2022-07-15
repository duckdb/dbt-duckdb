{% macro duckdb__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    {% if limit_num -%}
        regexp_replace(
          trim(cast(
            (array_agg(
              {{ measure }}
              {% if order_by_clause -%}
              {{ order_by_clause }}
              {%- endif %}
            )[1:{{ limit_num }}])
            as string), '[]'),
        ', ',
        {{ delimiter_text }},
        'g')
    {%- else %}
    string_agg(
        {{ measure }},
        {{ delimiter_text }}
        {% if order_by_clause -%}
        {{ order_by_clause }}
        {%- endif %}
        )
    {%- endif %}

{%- endmacro %}
