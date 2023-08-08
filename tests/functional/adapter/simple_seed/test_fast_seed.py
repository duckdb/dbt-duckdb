import pytest

from dbt.tests.adapter.simple_seed.test_seed import SeedTestBase
from dbt.tests.util import (
    run_dbt,
)

class TestSeedConfigFast(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "fast": True}
        }

    def test_simple_seed_fast(self, project):
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)