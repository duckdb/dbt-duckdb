import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

sources_schema_yml = """
version: 2
sources:
  - name: excel_source
    schema: main
    meta:
      plugin: excel
    tables:
      - name: excel_file
        description: "An excel file"
        meta:
          external_location: "{test_data_path}/excel_file.xlsx"
"""

models_source_model_sql = """
    select * from {{ source('excel_source', 'excel_file') }}
"""


@pytest.mark.skip_profile("buenavista")
class TestExcelPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        if "path" not in dbt_profile_target:
            return {}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target["path"],
                        "plugins": [{"name": "excel", "impl": "excel"}],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, test_data_path):
        return {
            "schema.yml": sources_schema_yml.format(test_data_path=test_data_path),
            "source_model.sql": models_source_model_sql,
            "source_model2.sql": models_source_model_sql,
            "source_model3.sql": models_source_model_sql,
            "source_model4.sql": models_source_model_sql,
        }

    def test_excel_plugin(self, project):
        results = run_dbt()
        assert len(results) == 4

        # relations_equal
        check_relations_equal(
            project.adapter,
            [
                "excel_file",
                "source_model",
                "source_model2",
                "source_model3",
                "source_model4",
            ],
        )
