{%- macro external_location(format) -%}
  {{- this.schema }}/{{ this.identifier}}.{{ format }}
{%- endmacro -%}
