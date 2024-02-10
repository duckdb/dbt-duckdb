{% materialization external, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set location = render(config.get('location', default=external_location(this, config))) -%})

  {%- set format = config.get('format', 'parquet') -%}

  {%- set target_relation = this.incorporate(type='view') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- has to be here
  {% call statement('main', language='sql') -%}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {%- set plugin_name = config.get('plugin') -%}

  {% do store_relation(plugin_name, target_relation, location, format, config) %}
  
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }} -- to what i return?

{% endmaterialization %}
