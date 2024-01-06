import pytest

from dbt.tests.util import (
    run_dbt,
)


# Skipping this b/c it requires running a properly setup Postgres server
# when testing it locally and also b/c I think there is something
# wrong with profiles_config_update since it can't be used in multiple
# tests in the same pytest session
#
# Exercise locally with: pytest --profile=file tests/functional/plugins/test_postgres.py
@pytest.mark.skip
class TestPostgresPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        config = {"dsn": "dbname=postgres", "sink_schema": "plugins", "overwrite": True}
        if "path" not in dbt_profile_target:
            return {}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target["path"],
                        "plugins": [{"module": "postgres", "config": config}],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"pg_model.sql": "SELECT * FROM plugins.foo"}

    def test_postgres_plugin(self, project):
        results = run_dbt()
        assert len(results) == 1

        res = project.run_sql("SELECT SUM(i) FROM pg_model", fetch="one")
        assert res[0] == 6