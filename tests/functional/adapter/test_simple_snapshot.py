import pytest

from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSnapshotCheck,
    BaseSimpleSnapshot,
)


pytestmark = pytest.mark.skip_database_type(
    "ducklake",
    reason="dbt snapshot execution still reaches a CASCADE drop that DuckLake rejects",
)


class TestSimpleSnapshotDuckDB(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckDuckDB(BaseSnapshotCheck):
    pass
