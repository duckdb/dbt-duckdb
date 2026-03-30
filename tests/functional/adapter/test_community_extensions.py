import pytest
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)

@pytest.mark.skip_profile("buenavista", "nightly", reason="Cannot install community extensions for nightly release")
class BaseCommunityExtensions:

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        dbt_profile_target["extensions"] = [
            {"name": "quack", "repo": "community"},
        ]
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "quack_model.sql": "select quack('world') as quack_world",
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_base(self, project):

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1

        # names exist in result nodes
        check_result_nodes_by_name(
            results,
            [
                "quack_model",
            ],
        )

        # check relation types
        expected = {
            "quack_model": "view",
        }
        check_relation_types(project.adapter, expected)

@pytest.mark.skip_profile("nightly", reason="Cannot install community extensions for nightly release")
@pytest.mark.skip_profile("buenavista")
class TestCommunityExtensions(BaseCommunityExtensions):
    pass
