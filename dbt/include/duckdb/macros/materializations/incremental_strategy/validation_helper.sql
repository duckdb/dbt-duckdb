{%- macro validate_string_field(field_value, field_name, errors) -%}
  {%- if field_value is not none and field_value is not string -%}
    {%- do errors.append(field_name ~ " must be a string, found: " ~ field_value) -%}
  {%- endif -%}
{%- endmacro -%}

{%- macro validate_string_list_field(field_value, field_name, errors) -%}
  {%- if field_value is not none -%}
    {%- if field_value is not sequence or field_value is mapping or field_value is string -%}
      {%- do errors.append(field_name ~ " must be a list") -%}
    {%- else -%}
      {%- for item in field_value -%}
        {%- if item is not string -%}
          {%- do errors.append(field_name ~ " must contain only string values, found: " ~ item) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}

{%- macro validate_dict_field(field_value, field_name, errors) -%}
  {%- if field_value is not none and field_value is not mapping -%}
    {%- do errors.append(field_name ~ " must be a dictionary, found: " ~ field_value) -%}
  {%- endif -%}
{%- endmacro -%}
