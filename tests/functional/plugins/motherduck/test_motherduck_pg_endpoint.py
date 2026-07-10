import pytest
from dbt.tests.util import run_dbt


model_sql = """
select *
from seed_table
"""

endpoint_seed_csv = """id,name
1,quack
2,dbt
"""


@pytest.mark.skip_profile("buenavista", "file", "memory", "md", "nightly")
class TestMotherDuckPgEndpoint:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, test_database_name):
        profile = {
            "type": "duckdb",
            "path": f"md:{test_database_name}",
            "token": dbt_profile_target.get("token"),
            "use_motherduck_postgres_endpoint": True,
            "disable_transactions": True,
        }
        for key in [
            "motherduck_pg_endpoint_region",
            "motherduck_pg_endpoint_host",
        ]:
            if key in dbt_profile_target:
                profile[key] = dbt_profile_target[key]

        return {
            "test": {
                "outputs": {
                    "dev": profile,
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"endpoint_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"endpoint_seed.csv": endpoint_seed_csv}

    @pytest.fixture(autouse=True)
    def setup(self, project):
        project.run_sql(
            "CREATE OR REPLACE TABLE seed_table AS SELECT 1 AS id, 'quack' AS name"
        )
        yield
        project.run_sql("DROP TABLE IF EXISTS endpoint_model")
        project.run_sql("DROP TABLE IF EXISTS endpoint_seed")
        project.run_sql("DROP TABLE IF EXISTS seed_table")

    def test_run_sql_model_through_pg_endpoint(self, project):
        results = run_dbt(["run"])

        assert len(results) == 1
        assert project.run_sql("SELECT * FROM endpoint_model", fetch="one") == (
            1,
            "quack",
        )

    def test_seed_through_pg_endpoint(self, project):
        results = run_dbt(["seed"])

        assert len(results) == 1
        assert project.run_sql(
            "SELECT id, name FROM endpoint_seed ORDER BY id",
            fetch="all",
        ) == [(1, "quack"), (2, "dbt")]
