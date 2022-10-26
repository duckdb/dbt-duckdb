{% materialization csv, adapter='duckdb' %}
  
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = this.incorporate(type='csv') %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% call statement('main') -%}
    {{ duckdb__materialize_as_csv(target_relation, sql) }}
    {%- endcall %}

    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
