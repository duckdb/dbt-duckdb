from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
)


class TestCachingLowerCaseModelDuckDB(BaseCachingLowercaseModel):
    pass


class TestCachingSelectedSchemaOnlyDuckDB(BaseCachingSelectedSchemaOnly):
    pass
