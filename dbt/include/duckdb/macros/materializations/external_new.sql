{% materialization external_new, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set target_relation = this.incorporate(type='view') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- has to be here
  {% call statement('main', language='sql') -%}
  {%- endcall %}

  {%- set location = render(config.get('location', default=external_location(this, config))) -%})
  -- just a check if the options is a dictionary to stay compielnt but it will be used over config in the plugins
  {%- set rendered_options = render_write_options(config) -%}
  {%- set format = config.get('format', 'parquet') -%}
  {%- set plugin_name = config.get('plugin', 'native') -%}

  {% do store_relation(plugin_name, target_relation, location, format, config) %}
  
  -- in this moment target should exists as a view so we can setup grants or docu

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }} -- to what i return?

{% endmaterialization %}
