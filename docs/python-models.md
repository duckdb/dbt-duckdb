# Python Models

dbt added support for [Python models in version 1.3.0](https://docs.getdbt.com/docs/build/python-models). In dbt-duckdb, Python models run in the **same process** that owns the DuckDB connection — no external compute cluster required.

## How it works

dbt-duckdb loads your `.py` model file as a Python module using `importlib`, then calls the `model` function you define. It passes two arguments:

- `dbt` — provides `ref()` and `source()` access to upstream models and sources
- `session` — a `DuckDBPyConnection` object for direct interaction with the database

The return value of `model` is materialized as a table in DuckDB.

## Supported return types

The `model` function can return any object that DuckDB knows how to convert to a table:

- Pandas `DataFrame`
- Polars `DataFrame`
- DuckDB `Relation`
- Arrow `Table`, `Dataset`, `RecordBatchReader`, or `Scanner`

## Using `dbt.ref` and `dbt.source`

Inside a Python model, `dbt.ref()` and `dbt.source()` return a [DuckDB Relation](https://duckdb.org/docs/api/python/reference/) object, which can be converted to any of the types above:

```python
def model(dbt, session):
    upstream = dbt.ref("my_upstream_model")

    # Convert to pandas
    df = upstream.df()

    # Or work with it as a DuckDB relation
    result = upstream.filter("amount > 100")
    return result
```

## Batch processing for large datasets

As of version 1.6.1, you can read and write data in chunks to process datasets that are larger than available memory. Use Arrow `RecordBatchReader` as both the source and the return type:

```python
import pyarrow as pa

def batcher(batch_reader: pa.RecordBatchReader):
    for batch in batch_reader:
        df = batch.to_pandas()
        # transform df...
        yield pa.RecordBatch.from_pandas(df)

def model(dbt, session):
    big_model = dbt.ref("big_model")
    batch_reader = big_model.record_batch(100_000)
    batch_iter = batcher(batch_reader)
    return pa.RecordBatchReader.from_batches(batch_reader.schema, batch_iter)
```

This streams data through the transformation without loading the full dataset into memory at once.
