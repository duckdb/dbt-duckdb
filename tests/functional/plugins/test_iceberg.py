import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

sources_schema_yml = """
version: 2
sources:
  - name: iceberg_source
    schema: main
    config:
      plugin: iceberg
      iceberg_table: "examples.{identifier}"
    tables:
      - name: nyc_taxi_locations
"""

models_source_model1_sql = """
    select * from {{ source('iceberg_source', 'nyc_taxi_locations') }}
"""


# Skipping this b/c it requires using my (@jwills) personal creds
# when testing it locally and also b/c I think there is something
# wrong with profiles_config_update since it can't be used in multiple
# tests in the same pytest session
@pytest.mark.skip
class TestIcebergPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        config = {"catalog": "default"}
        if "path" not in dbt_profile_target:
            return {}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target["path"],
                        "plugins": [
                            {"module": "iceberg", "config": config}
                        ],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources_schema_yml,
            "source_model1.sql": models_source_model1_sql,
        }

    def test_iceberg_plugin(self, project):
        results = run_dbt()
        assert len(results) == 1

        res = project.run_sql("SELECT COUNT(1) FROM nyc_taxi_locations", fetch="one")
        assert res[0] == 265

        check_relations_equal(
            project.adapter,
            [
                "nyc_taxi_locations",
                "source_model1",
            ],
        )