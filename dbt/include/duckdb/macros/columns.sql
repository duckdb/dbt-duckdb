{% macro duckdb__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns %}
    {% for column in add_columns %}
      {% set sql -%}
         alter {{ relation.type }} {{ relation }} add column
           {{ api.Relation.create(identifier=column.name) }} {{ column.data_type }}
      {%- endset -%}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}

  {% if remove_columns %}
    {% for column in remove_columns %}
      {% set sql -%}
        alter {{ relation.type }} {{ relation }} drop column
          {{ api.Relation.create(identifier=column.name) }}
      {%- endset -%}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro duckdb__alter_column_type(relation, column_name, new_column_type) -%}
  {#-- The default implementation adds a column, copies the data into it, drops the
       old column and renames the new one. DuckDB cannot commit a transaction that
       both updates a table's rows and alters its schema ("Attempting to modify table
       ... but another transaction has altered this table"), so change the type in a
       single ALTER instead, which DuckDB casts in place. --#}
  {% call statement('alter_column_type') %}
    alter table {{ relation.render() }} alter column {{ adapter.quote(column_name) }} type {{ new_column_type }}
  {% endcall %}
{%- endmacro %}
