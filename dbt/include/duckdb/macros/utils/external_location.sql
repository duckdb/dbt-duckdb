{%- macro external_location(format) -%}
  {{- adapter.external_root() }}/{{ this.identifier }}.{{ format }}
{%- endmacro -%}
