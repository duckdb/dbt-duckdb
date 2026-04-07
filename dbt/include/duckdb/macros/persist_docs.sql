
{#
  The logic in this file is adapted from dbt-postgres, since DuckDB matches
  the Postgres relation/column commenting model as of 0.10.1

  We intentionally diverge from the standard dbt/postgres persist_docs flow
  to support DuckLake safely. DuckLake comment statements may need to run
  after the model transaction commits, while the stock dbt persist_docs macro
  assumes comments always execute inline and relies on helpers such as
  run_query()/get_columns_in_relation() that can auto-open a transaction.

  That auto-begin behavior is harmless for standard DuckDB models, but it
  breaks the explicit post-commit path by silently re-entering a transaction.
  The helper macros below therefore own both the transaction routing and the
  post-commit column lookup so that `persist_docs.transaction: false` remains
  truly outside the model transaction.
#}

{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they do not use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro duckdb_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}

{% macro duckdb__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = duckdb_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}


{% macro duckdb__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = duckdb_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{% macro duckdb_get_persist_docs_config(model) %}
  {% set persist_docs = model.get('config', {}).get('persist_docs', {}) %}

  {% if persist_docs is not mapping %}
    {% do exceptions.raise_compiler_error(
      "Invalid value provided for 'persist_docs'. Expected dict but received " ~ persist_docs.__class__.__name__
    ) %}
  {% endif %}

  {% if 'transaction' in persist_docs and persist_docs['transaction'] is not boolean %}
    {% do exceptions.raise_compiler_error(
      "Invalid value provided for 'persist_docs.transaction'. Expected bool but received " ~ persist_docs['transaction']
    ) %}
  {% endif %}

  {{ return(persist_docs) }}
{% endmacro %}

{% macro duckdb_persist_docs_inside_transaction(relation, model) %}
  {% set persist_docs = duckdb_get_persist_docs_config(model) %}
  {{ return(adapter.persist_docs_inside_transaction(relation, persist_docs)) }}
{% endmacro %}

{% macro duckdb_get_existing_column_names(relation, inside_transaction=True) %}
  {% if inside_transaction %}
    {{ return(adapter.get_columns_in_relation(relation) | map(attribute="name") | list) }}
  {% endif %}

  {#
    Avoid adapter.get_columns_in_relation() here: the default dbt helper path
    opens a transaction, which would pull the "outside transaction" docs flow
    back into a transactional context and defeat the DuckLake workaround.
  #}
  {% call statement('duckdb_persist_docs_existing_columns', fetch_result=True, auto_begin=False) %}
    select column_name
    from system.information_schema.columns
    where table_name = '{{ relation.identifier }}'
      and lower(table_schema) = lower('{{ relation.schema }}')
      {% if relation.database %}
      and lower(table_catalog) = lower('{{ relation.database }}')
      {% endif %}
    order by ordinal_position
  {% endcall %}

  {{ return(load_result('duckdb_persist_docs_existing_columns').table.columns[0].values() | list) }}
{% endmacro %}

{% macro duckdb_validate_doc_columns(relation, column_dict, existing_column_names) %}
  {% set existing_lower = existing_column_names | map("lower") | list %}
  {% set missing = [] %}
  {% for col_name in column_dict %}
    {% if col_name | lower not in existing_lower %}
      {% do missing.append(col_name) %}
    {% endif %}
  {% endfor %}
  {% if missing | length > 0 %}
    {{ exceptions.warn("In relation " ~ relation.render() ~ ": The following columns are specified in the schema but are not present in the database: " ~ missing | join(", ")) }}
  {% endif %}
  {% set filtered = {} %}
  {% for col_name in column_dict if col_name | lower in existing_lower %}
    {% do filtered.update({col_name: column_dict[col_name]}) %}
  {% endfor %}
  {{ return(filtered) }}
{% endmacro %}

{% macro duckdb_get_column_comment_sql(relation, column_name, column_config) %}
  {% set escaped_comment = duckdb_escape_comment(column_config['description']) %}
  comment on column {{ relation }}.{{ adapter.quote(column_name) if column_config['quote'] else column_name }} is {{ escaped_comment }};
{% endmacro %}

{% macro duckdb_run_persist_docs(relation, model, inside_transaction=True) %}
  {% if duckdb_persist_docs_inside_transaction(relation, model) == inside_transaction %}
    {% if config.persist_relation_docs() and model.description %}
      {% call statement(auto_begin=inside_transaction) %}
        {{ alter_relation_comment(relation, model.description) }}
      {% endcall %}
    {% endif %}

    {% if config.persist_column_docs() and model.columns %}
      {% set existing_columns = duckdb_get_existing_column_names(relation, inside_transaction) %}
      {% set filtered_columns = duckdb_validate_doc_columns(relation, model.columns, existing_columns) %}
      {% for column_name in filtered_columns %}
        {% set alter_comment_sql = duckdb_get_column_comment_sql(relation, column_name, filtered_columns[column_name]) %}
        {% if alter_comment_sql and alter_comment_sql | trim | length > 0 %}
          {% call statement(auto_begin=inside_transaction) %}
            {{ alter_comment_sql }}
          {% endcall %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endmacro %}
