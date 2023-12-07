from dbt.tests.adapter.aliases.test_aliases import BaseAliases, BaseAliasErrors, BaseSameAliasDifferentSchemas

class TestAliasesDuckDB(BaseAliases):
    pass

class TestAliasesErrorDuckDB(BaseAliasErrors):
    pass

class BaseSameALiasDifferentSchemasDuckDB(BaseSameAliasDifferentSchemas):
    pass
