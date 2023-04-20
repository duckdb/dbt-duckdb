from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
)


class TestCachingLowerCaseModelDuckDB(BaseCachingLowercaseModel):
    pass


class TestCachingSelectedSchemaOnlyDuckDB(BaseCachingSelectedSchemaOnly):
    pass