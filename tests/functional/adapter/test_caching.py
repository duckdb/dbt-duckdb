import pytest

from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseCachingUppercaseModel,
    BaseNoPopulateCache,
)
from dbt.tests.util import run_dbt


class TestCachingLowerCaseModelDuckDB(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModelDuckDB(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnlyDuckDB(BaseCachingSelectedSchemaOnly):
    pass


class TestNoPopulateCacheDuckDB(BaseNoPopulateCache):
    pass


mixed_case_schema_model_sql = """
{{ config(materialized='table', schema='Staging') }}
select 1 as id
"""

mixed_case_database_model_sql = """
{{ config(materialized='table', database='Analytics') }}
select 1 as id
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestUnquotedMixedCaseRelations:
    """With quoting disabled, DuckDB keeps the case of the names dbt creates, so
    the relations found in the catalog on the next run must still match them."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "quoting": {"database": False, "schema": False, "identifier": False},
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "schema": unique_schema,
                        "attach": [{"path": ":memory:", "alias": "Analytics"}],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "Orders.sql": mixed_case_schema_model_sql,
            "Customers.sql": mixed_case_database_model_sql,
        }

    def test_rerun_finds_existing_relations(self, project):
        run_dbt(["run"])
        # the second run sees the tables created by the first one in the catalog
        results = run_dbt(["run"])
        assert len(results) == 2
