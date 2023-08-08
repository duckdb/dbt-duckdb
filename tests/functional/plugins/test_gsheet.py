import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

sources_schema_yml = """
version: 2
sources:
  - name: gsheet_source
    schema: main
    meta:
      plugin: gsheet
      title: "Josh's Test Spreadsheet"
    tables:
      - name: gsheet1
        description: "My first sheet"
      - name: gsheet2
        description: "The second sheet in the doc"
        meta:
          worksheet: "TwoSheet"
"""

models_source_model1_sql = """
    select * from {{ source('gsheet_source', 'gsheet1') }}
"""
models_source_model2_sql = """
    select * from {{ source('gsheet_source', 'gsheet2') }}
"""


# Skipping this b/c it requires using my (@jwills) personal creds
# when testing it locally and also b/c I think there is something
# wrong with profiles_config_update since it can't be used in multiple
# tests in the same pytest session
@pytest.mark.skip
class TestGSheetPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        config = {"method": "oauth"}
        if "path" not in dbt_profile_target:
            return {}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target["path"],
                        "plugins": [
                            {"module": "gsheet", "config": config}
                        ],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, test_data_path):
        return {
            "schema.yml": sources_schema_yml.format(test_data_path=test_data_path),
            "source_model1.sql": models_source_model1_sql,
            "source_model2.sql": models_source_model2_sql,
        }

    def test_gshseet_plugin(self, project):
        results = run_dbt()
        assert len(results) == 2

        check_relations_equal(
            project.adapter,
            [
                "gsheet1",
                "source_model1",
            ],
        )

        check_relations_equal(
            project.adapter,
            [
                "gsheet2",
                "source_model2",
            ],
        )
