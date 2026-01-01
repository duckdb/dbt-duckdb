{% materialization incremental, adapter="duckdb", supported_languages=['sql', 'python'] -%}

  {%- set language = model['language'] -%}
  
  {# Detect S3 Tables for Iceberg-specific SQL patterns #}
  {%- set database = config.get('database') -%}
  {%- set is_s3_tables = false -%}
  {%- if database -%}
    {%- set is_s3_tables = adapter.is_s3_tables_catalog(database) -%}
  {%- endif -%}
  
  -- only create temp tables if using local duckdb, as it is not currently supported for remote databases
  {%- set temporary = not adapter.is_motherduck() -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% set to_drop = [] %}
  {% if not temporary %}
    -- if not using a temporary table we will update the temp relation to use a different temp schema ("dbt_temp" by default)
    {% set temp_relation = temp_relation.incorporate(path=adapter.get_temp_relation_path(this)) %}
    {% do run_query(create_schema(temp_relation)) %}
    {% if not adapter.disable_transactions() %}
      {% do adapter.commit() %}
    {% endif %}
    -- then drop the temp relation after we insert the incremental data into the target relation
    {% do to_drop.append(temp_relation) %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, compiled_code, language) %}
    {% set need_swap = false %}
  {% elif full_refresh_mode %}
    {% if is_s3_tables %}
      {# S3 Tables: Use DROP + CREATE pattern (no CREATE OR REPLACE or DROP CASCADE support) #}
      {% if existing_relation is not none %}
        {% call statement('drop_existing_table') %}
          DROP TABLE {{ target_relation }}
        {% endcall %}
        {# Commit the DROP to ensure it's persisted before CREATE #}
        {% if not adapter.disable_transactions() %}
          {% do adapter.commit() %}
        {% endif %}
      {% endif %}
      
      {# Build CREATE TABLE statement #}
      {% set build_sql %}
        CREATE TABLE {{ target_relation }} AS (
          {{ compiled_code }}
        )
      {% endset %}
      
      {% set need_swap = false %}
    {% else %}
      {# Standard DuckDB: Use intermediate relation and swap #}
      {% set build_sql = create_table_as(False, intermediate_relation, compiled_code, language) %}
      {% set need_swap = true %}
    {% endif %}

  {% else %}
    {% if is_s3_tables %}
      {# S3 Tables: Use DELETE + INSERT pattern for incremental updates #}
      
      {# Require unique_key for DELETE + INSERT #}
      {%- if not unique_key -%}
        {{ exceptions.raise_compiler_error("Incremental models on S3 Tables (Iceberg) require a 'unique_key' for DELETE + INSERT operations. Either add unique_key config or use --full-refresh.") }}
      {%- endif -%}
      
      {# Check if watermark/precombine column is provided for deduplication #}
      {%- set watermark_column = config.get('watermark_column') or config.get('precombine_column') -%}
      
      {# Step 1: Create temp table with new data #}
      {% if language == 'python' %}
        {% set build_python = create_table_as(temporary, temp_relation, compiled_code, language) %}
        {% call statement("create_temp") %}
          {{- build_python }}
        {% endcall %}
      {% else %} {# SQL #}
        {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language)) %}
      {% endif %}
      
      {# Get target table columns using DESCRIBE (works with Iceberg metadata) #}
      {% call statement('get_s3_target_columns', fetch_result=True) %}
        DESCRIBE {{ target_relation }}
      {% endcall %}
      {% set target_describe = load_result('get_s3_target_columns').table %}
      {%- set target_cols_csv = [] -%}
      {%- set target_cols_raw = [] -%}
      {%- for row in target_describe.rows -%}
        {%- do target_cols_csv.append('"' ~ row[0] ~ '"') -%}
        {%- do target_cols_raw.append(row[0]) -%}
      {%- endfor -%}
      {%- set target_cols_csv_str = target_cols_csv | join(', ') -%}
      
      {# Get source (temp) table columns to detect schema changes #}
      {% call statement('get_s3_source_columns', fetch_result=True) %}
        DESCRIBE {{ temp_relation }}
      {% endcall %}
      {% set source_describe = load_result('get_s3_source_columns').table %}
      {%- set source_cols = [] -%}
      {%- for row in source_describe.rows -%}
        {%- do source_cols.append(row[0]) -%}
      {%- endfor -%}
      
      {# Check for schema changes - compare column names case-insensitively #}
      {%- set new_columns = [] -%}
      {%- set removed_columns = [] -%}
      
      {# Create lowercase versions for comparison #}
      {%- set target_cols_lower = [] -%}
      {%- for col in target_cols_raw -%}
        {%- do target_cols_lower.append(col | lower) -%}
      {%- endfor -%}
      
      {%- set source_cols_lower = [] -%}
      {%- for col in source_cols -%}
        {%- do source_cols_lower.append(col | lower) -%}
      {%- endfor -%}
      
      {# Find new columns (in source but not in target) #}
      {%- for col in source_cols -%}
        {%- if (col | lower) not in target_cols_lower -%}
          {%- do new_columns.append(col) -%}
        {%- endif -%}
      {%- endfor -%}
      
      {# Find removed columns (in target but not in source) #}
      {%- for col in target_cols_raw -%}
        {%- if (col | lower) not in source_cols_lower -%}
          {%- do removed_columns.append(col) -%}
        {%- endif -%}
      {%- endfor -%}
      
      {% if new_columns or removed_columns %}
        {# Schema change detected - Use PyIceberg to evolve schema #}
        {{ log("Schema change detected!", info=True) }}
        {% if new_columns %}
          {{ log("  New columns in source: " ~ new_columns, info=True) }}
        {% endif %}
        {% if removed_columns %}
          {{ log("  Columns removed from source (will be preserved with NULL values): " ~ removed_columns, info=True) }}
        {% endif %}
        
        {# Get column types from temp table for new columns #}
        {%- set new_cols_with_types = [] -%}
        {% if new_columns %}
          {% for row in source_describe.rows %}
            {% if row[0] in new_columns %}
              {% do new_cols_with_types.append({'name': row[0], 'type': row[1]}) %}
            {% endif %}
          {% endfor %}
        {% endif %}
        
        {# Call Python function to evolve schema - only adds columns, never drops #}
        {% set schema_evolved = adapter.evolve_s3_tables_schema(
            target_relation,
            add_columns=new_cols_with_types if new_cols_with_types else none,
            drop_columns=none
        ) %}
        
        {% if not schema_evolved %}
          {{ exceptions.raise_compiler_error("Failed to evolve schema using PyIceberg") }}
        {% endif %}
        
        {# Commit transaction to force DuckDB to refresh Iceberg metadata #}
        {% if not adapter.disable_transactions() %}
          {% do adapter.commit() %}
        {% endif %}
        
        {# Calculate refreshed columns: old columns + new columns (never subtract) #}
        {%- set target_cols_raw_refresh = [] -%}
        
        {# Keep ALL existing columns #}
        {%- for col in target_cols_raw -%}
          {%- do target_cols_raw_refresh.append(col) -%}
        {%- endfor -%}
        
        {# Add new columns #}
        {%- for col in new_columns -%}
          {%- do target_cols_raw_refresh.append(col) -%}
        {%- endfor -%}
        
        {# Build CSV string #}
        {%- set target_cols_csv_refresh = [] -%}
        {%- for col in target_cols_raw_refresh -%}
          {%- do target_cols_csv_refresh.append('"' ~ col ~ '"') -%}
        {%- endfor -%}
        {%- set target_cols_csv_str_refresh = target_cols_csv_refresh | join(', ') -%}
        
        {# Build SELECT with NULL padding for removed columns #}
        {%- set select_cols = [] -%}
        {%- for col in target_cols_raw_refresh -%}
          {%- if (col | lower) in source_cols_lower -%}
            {# Column exists in source #}
            {%- do select_cols.append('"' ~ col ~ '"') -%}
          {%- else -%}
            {# Column doesn't exist in source (was removed) - use NULL #}
            {%- do select_cols.append('NULL AS "' ~ col ~ '"') -%}
          {%- endif -%}
        {%- endfor -%}
        {%- set select_cols_str = select_cols | join(', ') -%}
      {% endif %}
      
      {# Step 2: Create staged table with deduplication logic (if watermark provided) #}
      {% if watermark_column %}
        {{ log("Applying watermark-based deduplication using column: " ~ watermark_column, info=True) }}
        {% set staged_relation = temp_relation.incorporate(path={"identifier": temp_relation.identifier ~ "_staged"}) %}
        
        {# Use refreshed columns if schema changed, otherwise use original #}
        {%- if new_columns or removed_columns -%}
          {%- set cols_for_staged = select_cols_str -%}
        {%- else -%}
          {%- set cols_for_staged = target_cols_csv_str -%}
        {%- endif -%}
        
        {% call statement("create_staged") %}
          CREATE OR REPLACE TEMP TABLE {{ staged_relation }} AS
          SELECT {{ cols_for_staged }}
          FROM (
            SELECT 
              *,
              ROW_NUMBER() OVER (
                PARTITION BY {{ unique_key }}
                ORDER BY {{ watermark_column }} DESC
              ) AS rn
            FROM {{ temp_relation }}
          ) ranked
          WHERE rn = 1
        {% endcall %}
      {% else %}
        {# No watermark - but if schema changed, create staged table with NULL padding #}
        {%- if new_columns or removed_columns -%}
          {% set staged_relation = temp_relation.incorporate(path={"identifier": temp_relation.identifier ~ "_staged"}) %}
          {% call statement("create_staged") %}
            CREATE OR REPLACE TEMP TABLE {{ staged_relation }} AS
            SELECT {{ select_cols_str }}
            FROM {{ temp_relation }}
          {% endcall %}
        {%- else -%}
          {# No schema changes - use temp table as-is #}
          {% set staged_relation = temp_relation %}
        {%- endif -%}
      {% endif %}
      
      {# Step 3: Check if we need to use PyIceberg for partitioned tables #}
      {%- set partition_by = config.get('partition_by') -%}
      {%- set use_pyiceberg = config.get('use_pyiceberg_writes', false) or partition_by -%}
      
      {%- if use_pyiceberg -%}
        {# Use PyIceberg DELETE + INSERT path for partitioned tables #}
        {# DuckDB doesn't support INSERT/DELETE on partitioned Iceberg tables #}
        {{ log("Using PyIceberg DELETE + INSERT for partitioned table (DuckDB limitation)", info=True) }}
        
        {# Call adapter method to perform DELETE + INSERT #}
        {%- set result = adapter.pyiceberg_incremental_write(target_relation, staged_relation, unique_key) -%}
        {{ log("PyIceberg DELETE + INSERT complete: " ~ result.rows_deleted ~ " deleted, " ~ result.rows_inserted ~ " inserted", info=True) }}
        
        {# Generate SQL representation for compiled output (actual operation happens in Python) #}
        {% set build_sql %}
          -- PyIceberg DELETE + INSERT operation (executed via Python, not SQL)
          -- This is a representation of the operation for documentation purposes
          -- Actual execution: {{ result.rows_deleted }} rows deleted, {{ result.rows_inserted }} rows inserted
          
          -- Step 1: Delete existing rows matching unique key
          -- DELETE FROM {{ target_relation }}
          -- WHERE {{ unique_key }} IN (
          --   SELECT DISTINCT {{ unique_key }} FROM {{ staged_relation }}
          -- );
          
          -- Step 2: Insert new rows
          -- INSERT INTO {{ target_relation }}
          -- SELECT * FROM {{ staged_relation }};
          
          -- Note: Actual operation performed via PyIceberg transaction for atomicity
          SELECT 1 as pyiceberg_operation_complete
        {% endset %}
        
      {%- else -%}
        {# Use standard DuckDB DELETE + INSERT for non-partitioned tables #}
        {# Use refreshed columns if schema changed, otherwise use original #}
        {%- if new_columns or removed_columns -%}
          {%- set insert_cols_str = target_cols_csv_str_refresh -%}
          {%- set select_cols_final = select_cols_str -%}
        {%- else -%}
          {%- set insert_cols_str = target_cols_csv_str -%}
          {%- set select_cols_final = target_cols_csv_str -%}
        {%- endif -%}
        
        {% set build_sql %}
          DELETE FROM {{ target_relation }}
          WHERE {{ unique_key }} IN (
            SELECT DISTINCT {{ unique_key }} FROM {{ staged_relation }}
          );
          
          INSERT INTO {{ target_relation }} ({{ insert_cols_str }})
          SELECT {{ select_cols_final }}
          FROM {{ staged_relation }}
        {% endset %}
      {%- endif -%}
      
      {% set need_swap = false %}
      {% set language = "sql" %}
    {% else %}
      {# Standard DuckDB incremental logic #}
      {% if language == 'python' %}
        {% set build_python = create_table_as(temporary, temp_relation, compiled_code, language) %}
        {% call statement("pre", language=language) %}
          {{- build_python }}
        {% endcall %}
      {% else %} {# SQL #}
        {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language)) %}
      {% endif %}
      {% do adapter.expand_target_column_types(
               from_relation=temp_relation,
               to_relation=target_relation) %}
      {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
      {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
      {% if not dest_columns %}
        {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
      {% endif %}

      {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
      {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
      {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
      {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
      {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
      {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}
      {% set language = "sql" %}
      {% set need_swap = false %}
    {% endif %}

  {% endif %}

  {% call statement("main", language=language) %}
      {{- build_sql }}
  {% endcall %}

  {# Set up partitioning and properties for S3 Tables full refresh #}
  {%- if is_s3_tables and full_refresh_mode -%}
    {# Set up partitioning if provided (DuckDB 1.4.2+) #}
    {%- set partition_by = config.get('partition_by') -%}
    {%- if partition_by -%}
      {{ log("Setting up Iceberg table partitioning using PyIceberg", info=True) }}
      {%- set partition_updated = adapter.update_s3_tables_partitioning(target_relation, partition_by) -%}
      {%- if not partition_updated -%}
        {{ exceptions.raise_compiler_error("Failed to set up table partitioning") }}
      {%- endif -%}
      {# Commit transaction to force DuckDB to refresh Iceberg metadata #}
      {% if not adapter.disable_transactions() %}
        {% do adapter.commit() %}
      {% endif %}
    {%- endif -%}
    
    {# Set Iceberg table properties if provided (DuckDB 1.4.2+) #}
    {%- set iceberg_properties = config.get('iceberg_properties', {}) -%}
    {%- if iceberg_properties -%}
      {# Build properties dict for set_iceberg_table_properties #}
      {%- set props_dict = [] -%}
      {%- for key, value in iceberg_properties.items() -%}
        {%- do props_dict.append("'" ~ key ~ "': '" ~ value ~ "'") -%}
      {%- endfor -%}
      {% call statement('set_iceberg_properties') %}
        CALL set_iceberg_table_properties({{ target_relation }}, { {{ props_dict | join(', ') }} })
      {% endcall %}
    {%- endif -%}
  {%- endif -%}

  {% if need_swap %}
      {#-- Drop indexes on target relation before renaming to backup to avoid dependency errors --#}
      {% do drop_indexes_on_relation(target_relation) %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {# Align order with table materialization to avoid MotherDuck alter conflicts #}
  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {# On MotherDuck the temp relation is a real table; dropping it cascades indexes. Avoid extra ALTERs. #}
      {% if not adapter.is_motherduck() %}
        {% do drop_indexes_on_relation(rel) %}
      {% endif %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
