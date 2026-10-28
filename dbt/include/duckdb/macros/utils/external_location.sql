{%- macro external_location(relation, config) -%}
  {#- option names are case-insensitive, as they are in DuckDB's COPY statement -#}
  {%- set options = config.get('options', {}) -%}
  {%- set ns = namespace(partition_by=none) -%}
  {%- for key in options if key | lower == 'partition_by' -%}
    {%- set ns.partition_by = options[key] -%}
  {%- endfor -%}
  {%- if ns.partition_by is none -%}
    {%- set format = config.get('format', 'parquet') -%}
    {{- adapter.external_root() }}/{{ relation.identifier }}.{{ format }}
  {%- else -%}
    {{- adapter.external_root() }}/{{ relation.identifier }}
  {%- endif -%}
{%- endmacro -%}
