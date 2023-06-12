from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    pass


class TestIncrementalPredicates(BaseIncrementalPredicates):
    pass


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass
