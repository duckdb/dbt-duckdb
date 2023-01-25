{%- macro external_location(relation, format) -%}
  {{- adapter.external_root() }}/{{ relation.identifier }}.{{ format }}
{%- endmacro -%}
