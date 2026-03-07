# Writing External Files

dbt-duckdb supports an `external` materialization strategy that writes model output directly to a file (local or remote) instead of a database table.

## Basic usage

```sql
{{ config(materialized='external', location='local/directory/file.parquet') }}

SELECT m.*, s.id IS NOT NULL as has_source_id
FROM {{ ref('upstream_model') }} m
LEFT JOIN {{ source('upstream', 'source') }} s USING (id)
```

## Configuration options

| Option | Default | Description |
| --- | --- | --- |
| `location` | Auto-generated (see below) | Path or S3 URI to write the file to |
| `format` | `parquet` | Output format: `parquet`, `csv`, or `json` |
| `delimiter` | `,` | Field delimiter for CSV output |
| `options` | None | Additional options passed to DuckDB's `COPY` statement (e.g., `partition_by`, `codec`) |
| `glue_register` | `false` | Register the output file with the AWS Glue Catalog |
| `glue_database` | `default` | AWS Glue database to register the model in |

## Location inference

If `location` is specified, dbt-duckdb infers `format` from the file extension when `format` is not set (e.g., `.parquet` → `parquet`, `.csv` → `csv`). This inference was added in version 1.4.1.

If `location` is not specified, the output file is named after the model file (e.g., `my_model.parquet`) and written relative to the current working directory. You can change the default output directory by setting `external_root` in your DuckDB profile:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      external_root: s3://my-bucket/dbt-outputs/
  target: dev
```

## Limitations

Incremental materialization strategies (`delete+insert`, `append`, `merge`, `microbatch`) are not currently supported for `external` models.
