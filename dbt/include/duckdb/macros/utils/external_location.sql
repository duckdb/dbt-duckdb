{%- macro external_location(relation, config) -%}
  {{- adapter.external_root() }}/{{ relation.identifier }}
{%- endmacro -%}
