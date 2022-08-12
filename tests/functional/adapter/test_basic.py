import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
)
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    no_stats,
    expected_references_catalog,
)


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


class TestBaseAdapterMethodDuckDB(BaseAdapterMethod):
    pass


class TestValidateConnectionDuckDB(BaseValidateConnection):
    pass


class TestDocsGenerateDuckDB(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=None,
            id_type="INTEGER",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )


class TestDocsGenReferencesDuckDB(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role=None,
            id_type="INTEGER",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
            bigint_type="BIGINT",
        )
