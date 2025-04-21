from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.artifacts.schemas.results import RunStatus


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    def test__bad_unique_key_list(self, project):
        """expect compilation error from unique key not being a column"""

        (status, exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key_list"
        )

        assert status == RunStatus.Error
        # MotherDuck has a `dbt_temp` workaround for incremental runs which causes this test to fail
        # because the error message is being truncated with DuckDB >= 1.2.0
        if not project.adapter.config.credentials.is_motherduck:
            assert "thisisnotacolumn" in exc.lower()


class TestIncrementalPredicates(BaseIncrementalPredicates):
    pass


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass
