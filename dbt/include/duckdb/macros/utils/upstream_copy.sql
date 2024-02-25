-- todo: this would not work if two external are in a row
{%- macro register_upstream_external_model_copy() -%}
{% if execute %}
{% set upstream_nodes = {} %}
{% for node in selected_resources %}
  {% for upstream_node in graph['nodes'][node]['depends_on']['nodes'] %}
    {% if upstream_node not in upstream_nodes and upstream_node not in selected_resources %}
      {% do upstream_nodes.update({upstream_node: None}) %}
      {% set upstream = graph['nodes'].get(upstream_node) %}
      {% if upstream
         and upstream.resource_type in ('model', 'seed')
         and upstream.config.materialized=='external'
      %}
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
      {%- endif %}
    {% endif %}
  {% endfor %}
{% endfor %}
{% do adapter.commit() %}
{% endif %}
{%- endmacro -%}
