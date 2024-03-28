-- todo: this would not work if two external are in a row
{%- macro register_upstream_external_models() -%}
{% if execute %}
{% set upstream_nodes = {} %}
{% for node in selected_resources %}
  {% for upstream_node in graph['nodes'][node]['depends_on']['nodes']
    if upstream_node not in upstream_nodes and upstream_node not in selected_resources
    and graph['nodes'].get(upstream_node).resource_type in ('model', 'seed')
    and graph['nodes'].get(upstream_node).config.materialized=='external'
  %}
      {% do upstream_nodes.update({upstream_node: None}) %}
      {% set upstream = graph['nodes'].get(upstream_node) %}
      {%- set upstream_rel = api.Relation.create(
        database=upstream['database'],
        schema=upstream['schema'],
        identifier=upstream['alias']
      ) -%}
      {%- set rendered_options = render_write_options(config) -%}
      {%- set location = upstream.config.get('location', external_location(upstream_rel, upstream.config)) -%}
      {%- set format = upstream.config.get('format', 'default') -%}
      {%- set plugin_name = upstream.config.get('plugin', 'native') -%}
      {% do store_relation(plugin_name, upstream_rel, location, format, upstream.config, True) %}
  {% endfor %}
{% endfor %}
{% do adapter.commit() %}
{% endif %}
{%- endmacro -%}



{%- macro register_self_reference_external_models() -%}
{% if execute and var("first_run") != 'true' %}
{% for node in selected_resources if
    graph['nodes'][node].resource_type in ('model', 'seed')
    and graph['nodes'][node].config.materialized=='external'
  %}
    {% set current_node = graph['nodes'][node] %}
    {%- set node_rel = api.Relation.create(
      database=current_node['database'],
      schema=current_node['schema'],
      identifier=current_node['alias']
    ) -%}
    {%- set rendered_options = render_write_options(config) -%}
    {%- set location = current_node.config.get('location', external_location(node_rel, current_node.config)) -%}
    {%- set format = current_node.config.get('format', 'default') -%}
    {%- set plugin_name = current_node.config.get('plugin', 'native') -%}
    {% do store_relation(plugin_name, node_rel, location, format, current_node.config, True) %}
{% endfor %}
{% do adapter.commit() %}
{% endif %}
{%- endmacro -%}
