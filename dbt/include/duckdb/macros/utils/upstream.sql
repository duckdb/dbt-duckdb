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
        {% call statement('main', language='sql') -%}
          create or replace view {{ upstream_rel.include(database=False) }} as (
            select * from '{{ upstream_location }}'
          );
        {%- endcall %}
      {%- endif %}
    {% endif %}
  {% endfor %}
{% endif %}
{%- endmacro -%}
