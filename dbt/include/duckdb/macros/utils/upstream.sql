{%- macro register_external_upstream() -%}
{% if execute %}
  {% for node in model['depends_on']['nodes'] %}
    {% set upstream = graph.nodes.get(node) %}
    {% if upstream %}
      {%- set upstream_rel = api.Relation.create(
        database=upstream['database'],
        schema=upstream['schema'],
        identifier=upstream['alias']
      ) -%}
      {%- if upstream.config.materialized=='external' and not load_cached_relation(upstream_rel) -%}
          {%- set format = render(upstream.config.get('format', 'parquet')) -%}
          {%- set upstream_location = render(
            upstream.config.get('location', external_location(upstream_rel, format)))
          -%}
          {{ adapter.create_external_upstream_relation(upstream_rel, upstream_location) }}
      {%- endif %}
    {% endif %}
  {% endfor %}
{% endif %}
{%- endmacro -%}
