{%- macro external_read_format(config, rendered_options) -%}
  {#- The format an external model's files are read back in: the configured
      format, else the one external_write_options picked for the write -#}
  {%- set allowed_formats = ['csv', 'parquet', 'json'] -%}
  {%- set format = config.get('format') -%}
  {%- if format -%}
    {%- if format not in allowed_formats -%}
      {{ exceptions.raise_compiler_error("Invalid format: " ~ format ~ ". Allowed formats are: " ~ allowed_formats | join(', ')) }}
    {%- endif -%}
  {%- else -%}
    {%- set format = rendered_options['format'] | lower -%}
    {%- set format = format if format in allowed_formats else 'parquet' -%}
  {%- endif -%}
  {{ return(format) }}
{%- endmacro -%}

{%- macro external_read_function(read_location, format, config, rendered_options) -%}
  {#- The read_csv/read_json/read_parquet call that reads an external model's
      files back with the model's read options. DuckDB detects compression from
      a lower-case extension only, so the write compression is passed to the
      CSV and JSON readers. -#}
  {%- if format == 'json' -%}
    {%- set read_options = config.get('json_read_options', {'auto_detect': True}) -%}
  {%- elif format == 'csv' -%}
    {%- set read_options = config.get('csv_read_options', {'auto_detect': True}) -%}
  {%- else -%}
    {%- set read_options = config.get('parquet_read_options', {'union_by_name': False}) -%}
  {%- endif -%}
  {%- if format != 'parquet' and rendered_options.get('compression') and 'compression' not in read_options -%}
    {%- set read_options = dict(read_options, compression=rendered_options['compression']) -%}
  {%- endif -%}
  read_{{ format }}('{{ read_location }}'
  {%- for key, value in read_options.items() -%}
    , {{ key }}=
    {%- if value is string -%}
      '{{ value }}'
    {%- else -%}
      {{ value }}
    {%- endif -%}
  {%- endfor -%}
  )
{%- endmacro -%}
