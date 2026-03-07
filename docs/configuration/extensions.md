# Extensions, Settings & Secrets

## DuckDB Extensions

You can install and load any [DuckDB extension](https://duckdb.org/docs/extensions/overview) by listing it under `extensions` in your profile:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - httpfs
        - parquet
  target: dev
```

To use a **community** or **nightly** extension from outside the core repository, provide the extension as a `name`/`repo` pair:

```yaml
extensions:
  - httpfs
  - parquet
  - name: h3
    repo: community
  - name: uc_catalog
    repo: core_nightly
```

## DuckDB Settings

You can set any [DuckDB configuration option](https://duckdb.org/docs/sql/configuration) via the `settings` field, including options exposed by loaded extensions:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      settings:
        threads: 4
        memory_limit: "4GB"
  target: dev
```

## Secrets Manager

To use the [DuckDB Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html), configure the `secrets` field. For example, to authenticate with AWS S3:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      extensions:
        - httpfs
        - parquet
      secrets:
        - type: s3
          region: my-aws-region
          key_id: "{{ env_var('S3_ACCESS_KEY_ID') }}"
          secret: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
  target: dev
```

### Credential chain (IAM / web identity)

Instead of providing credentials directly, you can use the `CREDENTIAL_CHAIN` provider to fetch them automatically from AWS (e.g., via web identity tokens, instance profiles, or environment variables):

```yaml
secrets:
  - type: s3
    provider: credential_chain
```

### Scoped credentials

Secrets can be scoped so that different storage paths use different credentials. When looking up a secret for a given path, dbt-duckdb picks the secret whose scope is the longest matching prefix:

```yaml
secrets:
  - type: s3
    provider: credential_chain
    scope:
      - "s3://bucket-in-eu-region"
      - "s3://bucket-2-in-eu-region"
    region: "eu-central-1"
  - type: s3
    region: us-west-2
    scope: "s3://bucket-in-us-region"
```

## fsspec Filesystems

As of version 1.4.1, dbt-duckdb has experimental support for [fsspec](https://duckdb.org/docs/guides/python/filesystems.html)-based filesystems. This gives DuckDB access to cloud storage (S3, GCS, Azure Blob Storage, etc.) via the `fsspec` ecosystem.

Configure filesystems under the `filesystems` key. Each entry must include an `fs` property naming the fsspec protocol, plus any additional key-value pairs required by that implementation:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      filesystems:
        - fs: s3
          anon: false
          key: "{{ env_var('S3_ACCESS_KEY_ID') }}"
          secret: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
          client_kwargs:
            endpoint_url: "http://localhost:4566"
  target: dev
```

Install the relevant fsspec implementation before using it (e.g., `pip install s3fs`, `pip install gcsfs`, `pip install adlfs`).
