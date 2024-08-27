{% materialization external_table, adapter="duckdb", supported_languages=['sql', 'python'] %}
{{ log("External macro") }}

{%- set target_relation = this.incorporate(type='view') %}

{%- set plugin_name = config.get('plugin') -%}
{%- set location = render(config.get('location', default=external_location(this, config))) -%})
{%- set format = config.get('format', 'parquet') -%}

{% do store_relation(plugin_name, target_relation, location, format, config) %}

{% call statement('main', language='sql') -%}

{%- endcall %}

-- we have to load this table as df and create target_relation view

{{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
