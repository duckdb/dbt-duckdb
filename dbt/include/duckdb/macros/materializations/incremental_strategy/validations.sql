{% macro validate_merge_update_options(update_all, update_by_name, update_by_position) %}
  {%- set update_options = [] -%}
  {%- if update_all -%}{%- do update_options.append('merge_update_all') -%}{%- endif -%}
  {%- if update_by_name -%}{%- do update_options.append('merge_update_by_name') -%}{%- endif -%}
  {%- if update_by_position -%}{%- do update_options.append('merge_update_by_position') -%}{%- endif -%}

  {%- if update_options | length > 1 -%}
    {{ exceptions.raise_compiler_error(
      "Conflicting update options: " ~ update_options | join(', ') ~
      ". Only one update method can be specified."
    ) }}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_insert_options(insert_all, insert_by_name, insert_by_position) %}
  {%- set insert_options = [] -%}
  {%- if insert_all -%}{%- do insert_options.append('merge_insert_all') -%}{%- endif -%}
  {%- if insert_by_name -%}{%- do insert_options.append('merge_insert_by_name') -%}{%- endif -%}
  {%- if insert_by_position -%}{%- do insert_options.append('merge_insert_by_position') -%}{%- endif -%}

  {%- if insert_options | length > 1 -%}
    {{ exceptions.raise_compiler_error(
      "Conflicting insert options: " ~ insert_options | join(', ') ~
      ". Only one insert method can be specified."
    ) }}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_using_clause(use_using_clause, using_columns) %}
  {%- if use_using_clause and not using_columns -%}
    {{ exceptions.raise_compiler_error(
      "'merge_using_columns' must be specified when 'merge_use_using_clause' is true. " ~
      "Example: merge_using_columns=['id', 'updated_at']"
    ) }}
  {%- endif -%}

  {%- if using_columns and not use_using_clause -%}
    {{ exceptions.raise_compiler_error(
      "'merge_use_using_clause' must be set to true when 'merge_using_columns' is specified"
    ) }}
  {%- endif -%}

  {%- if using_columns and not (using_columns is sequence and using_columns is not string) -%}
    {{ exceptions.raise_compiler_error(
      "'merge_using_columns' must be a list of column names. " ~
      "Example: merge_using_columns=['id', 'updated_at']"
    ) }}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_action_values(matched_action, not_matched_action, not_matched_by_source) %}
  {%- set valid_matched_actions = ['update', 'delete', 'do_nothing'] -%}
  {%- if matched_action not in valid_matched_actions -%}
    {{ exceptions.raise_compiler_error(
      "'merge_matched_action' must be one of: " ~ valid_matched_actions | join(', ') ~
      ". Got: '" ~ matched_action ~ "'"
    ) }}
  {%- endif -%}

  {%- set valid_not_matched_actions = ['insert', 'do_nothing'] -%}
  {%- if not_matched_action not in valid_not_matched_actions -%}
    {{ exceptions.raise_compiler_error(
      "'merge_not_matched_action' must be one of: " ~ valid_not_matched_actions | join(', ') ~
      ". Got: '" ~ not_matched_action ~ "'"
    ) }}
  {%- endif -%}

  {%- if not_matched_by_source -%}
    {%- set valid_not_matched_by_source = ['delete', 'update', 'do_nothing'] -%}
    {%- if not_matched_by_source is string and not_matched_by_source not in valid_not_matched_by_source -%}
      {{ exceptions.raise_compiler_error(
        "'when_not_matched_by_source' must be one of: " ~ valid_not_matched_by_source | join(', ') ~
        " or a mapping with 'update_columns' and 'update_values'. Got: '" ~ not_matched_by_source ~ "'"
      ) }}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_column_configs(update_all, update_by_name, update_by_position, update_columns, exclude_columns) %}
  {%- set has_auto_update = update_all or update_by_name or update_by_position -%}

  {%- if has_auto_update and update_columns -%}
    {{ exceptions.raise_compiler_error(
      "Cannot specify 'merge_update_columns' when using automatic update options " ~
      "('merge_update_all', 'merge_update_by_name', or 'merge_update_by_position')"
    ) }}
  {%- endif -%}

  {%- if has_auto_update and exclude_columns -%}
    {{ exceptions.raise_compiler_error(
      "Cannot specify 'merge_exclude_columns' when using automatic update options " ~
      "('merge_update_all', 'merge_update_by_name', or 'merge_update_by_position')"
    ) }}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_error_config(config_name, config_value) %}
  {%- if config_value -%}
    {%- if not config_value is mapping -%}
      {{ exceptions.raise_compiler_error(
        "'" ~ config_name ~ "' must be a dictionary with 'condition' and/or 'message' keys. " ~
        "Example: " ~ config_name ~ "={'condition': 'target.status = \"locked\"', 'message': 'Cannot update locked records'}"
      ) }}
    {%- endif -%}

    {%- set valid_keys = ['condition', 'message'] -%}
    {%- for key in config_value.keys() -%}
      {%- if key not in valid_keys -%}
        {{ exceptions.raise_compiler_error(
          "Invalid key '" ~ key ~ "' in " ~ config_name ~ ". Valid keys are: " ~ valid_keys | join(', ')
        ) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_custom_update_mapping(not_matched_by_source) %}
  {%- if not_matched_by_source is mapping -%}
    {%- if not not_matched_by_source.update_columns -%}
      {{ exceptions.raise_compiler_error(
        "'when_not_matched_by_source' mapping must include 'update_columns' key. " ~
        "Example: when_not_matched_by_source={'update_columns': ['status'], 'update_values': {'status': '\"inactive\"'}}"
      ) }}
    {%- endif -%}

    {%- if not (not_matched_by_source.update_columns is sequence and not_matched_by_source.update_columns is not string) -%}
      {{ exceptions.raise_compiler_error(
        "'update_columns' must be a list of column names"
      ) }}
    {%- endif -%}

    {%- if not_matched_by_source.update_values and not not_matched_by_source.update_values is mapping -%}
      {{ exceptions.raise_compiler_error(
        "'update_values' must be a dictionary mapping column names to values"
      ) }}
    {%- endif -%}

    {%- if not_matched_by_source.update_values -%}
      {%- set missing_values = [] -%}
      {%- for col in not_matched_by_source.update_columns -%}
        {%- if col not in not_matched_by_source.update_values -%}
          {%- do missing_values.append(col) -%}
        {%- endif -%}
      {%- endfor -%}
      {%- if missing_values -%}
        {{ log(
          "Warning: Columns " ~ missing_values | join(', ') ~
          " in 'update_columns' have no corresponding values in 'update_values'. They will be set to NULL.",
          info=true
        ) }}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_matched_and_not_matched_actions(matched_action, update_all, update_by_name, update_by_position) %}
  {%- if matched_action != 'update' and (update_all or update_by_name or update_by_position) -%}
    {{ exceptions.raise_compiler_error(
      "Update options ('merge_update_all', 'merge_update_by_name', 'merge_update_by_position') " ~
      "can only be used when 'merge_matched_action' is 'update'. " ~
      "Current merge_matched_action: '" ~ matched_action ~ "'"
    ) }}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_not_matched_and_insert_options(not_matched_action, insert_all, insert_by_name, insert_by_position) %}
  {%- if not_matched_action != 'insert' and (insert_all or insert_by_name or insert_by_position) -%}
    {{ exceptions.raise_compiler_error(
      "Insert options ('merge_insert_all', 'merge_insert_by_name', 'merge_insert_by_position') " ~
      "can only be used when 'merge_not_matched_action' is 'insert'. " ~
      "Current merge_not_matched_action: '" ~ not_matched_action ~ "'"
    ) }}
  {%- endif -%}
{% endmacro %}

{% macro validate_merge_unique_key_and_using_clause(unique_key, use_using_clause, using_columns) %}
  {%- if not unique_key and not (use_using_clause and using_columns) -%}
    {{ log(
      "Warning: No unique_key specified and no USING clause configured. " ~
      "This will result in a cartesian join (ON FALSE). " ~
      "Consider specifying either 'unique_key' or using 'merge_use_using_clause' with 'merge_using_columns'.",
      info=true
    ) }}
  {%- endif -%}
{% endmacro %}
