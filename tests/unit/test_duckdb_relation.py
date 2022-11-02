import pytest

from dbt.adapters.duckdb.relation import _create_external_location, DuckDBRelation
from dbt.contracts.graph.unparsed import ExternalTable

def test_csv_auto_with_delimiter():
    external = ExternalTable(
        location="s3://bucket/path/to/data",
        file_format="csv",
        extra={"options": {"delimiter": "','"}},
    )
    assert _create_external_location(external) == "read_csv_auto('s3://bucket/path/to/data', delimiter = ',')"

def test_parquet_with_filename():
    external = ExternalTable(
        location="s3://bucket/path/to/data",
        file_format="parquet",
        extra={"options": {"filename": 1}},
    )
    assert _create_external_location(external) == "read_parquet('s3://bucket/path/to/data', filename = 1)"