import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp

class TestSimpleMaterializationsDuckDB(BaseSimpleMaterializations):
    pass


class TestSingularTestsDuckDB(BaseSingularTests):
    pass


class TestSingularTestsEphemeralDuckDB(BaseSingularTestsEphemeral):
    pass


class TestEmptyDuckDB(BaseEmpty):
    pass


class TestEphemeralDuckDB(BaseEphemeral):
    pass


class TestIncrementalDuckDB(BaseIncremental):
    pass


class TestGenericTestsDuckDB(BaseGenericTests):
    pass


class TestSnapshotCheckColsDuckDB(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampDuckDB(BaseSnapshotTimestamp):
    pass
