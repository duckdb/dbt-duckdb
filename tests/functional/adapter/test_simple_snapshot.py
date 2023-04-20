import pytest
from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSnapshotCheck, BaseSimpleSnapshot


@pytest.mark.skip
class TestSimpleSnapshotDuckDB(BaseSimpleSnapshot):
    pass

class TestSnapshotCheckDuckDB(BaseSnapshotCheck):
    pass