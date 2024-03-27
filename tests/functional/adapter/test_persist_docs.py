import pytest

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)

@pytest.mark.skip_profile("md")
class TestPersistDocs(BasePersistDocs):
    pass


@pytest.mark.skip_profile("md")
class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


@pytest.mark.skip_profile("md")
class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
