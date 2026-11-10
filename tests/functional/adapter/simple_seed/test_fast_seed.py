import datetime

import pytest

from dbt.tests.adapter.simple_seed.test_seed import SeedTestBase
from dbt.tests.adapter.simple_seed.test_seed import SeedUniqueDelimiterTestBase
from dbt.tests.util import (
    run_dbt,
)

seeds__null_literals_csv = """id,amount,name,signup_date
1,10,alice,2024-01-01
2,null,null,null
3,NULL,NULL,NULL
4,Null,Null,Null
5,,,
6,"null","null","null"
""".lstrip()

seeds__null_literals_yml = """
seeds:
  - name: agate_nulls
    config:
      fast: false
"""

class TestSeedConfigFast(SeedTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "fast": True}
        }

    def test_simple_seed_fast(self, project):
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class TestSeedWithUniqueDelimiter(SeedUniqueDelimiterTestBase):
    def test_seed_with_unique_delimiter(self, project):
        """Testing correct run of seeds with a unique delimiter (pipe in this case)"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class TestSeedWithWrongDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ";"},
        }

    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "syntax error" in seed_result.results[0].message.lower()


class TestSeedWithEmptyDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ""},
        }

    def test_seed_with_empty_delimiter(self, project):
        """Testing failure of running dbt seed with an empty configured delimiter value"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "compilation error" in seed_result.results[0].message.lower()


class TestSeedNullLiterals:
    """dbt reads 'null' and empty cells in a seed file as NULL; the COPY fast path must agree."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "fast_nulls.csv": seeds__null_literals_csv,
            "agate_nulls.csv": seeds__null_literals_csv,
            "properties.yml": seeds__null_literals_yml,
        }

    def test_null_literals_load_as_null(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 2

        for seed in ("fast_nulls", "agate_nulls"):
            rows = project.run_sql(
                f"""
                select id, amount, name, signup_date
                from {project.test_schema}.{seed}
                order by id
                """,
                fetch="all",
            )
            assert rows[0] == (1, 10, "alice", datetime.date(2024, 1, 1)), seed
            for row in rows[1:]:
                assert row[1:] == (None, None, None), (seed, row)
