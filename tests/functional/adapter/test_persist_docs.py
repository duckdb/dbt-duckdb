import pytest

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
# DuckLake support is merged and will be available in a future release:
# https://github.com/duckdb/ducklake/pull/1102


@pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support comments on view columns"
)
class TestPersistDocs(BasePersistDocs):
    pass


@pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support comments on view columns"
)
class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


@pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support comments on view columns"
)
class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
