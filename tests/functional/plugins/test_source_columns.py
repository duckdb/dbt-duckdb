import pytest

from dbt.tests.util import run_dbt

schema_yml = """
version: 2
sources:
  - name: plugin_source
    schema: main
    config:
      plugin: source_columns
      path: "{csv_path}"
    tables:
      - name: allowance
        columns:
          - name: allow_company
            data_type: text
          - name: allow_pointer
            data_type: text
"""

model_sql = """
    select allow_company, allow_pointer from {{ source('plugin_source', 'allowance') }}
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestSourceColumns:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        plugins = [{"module": "tests.source_columns_plugin", "alias": "source_columns"}]
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": plugins,
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def headerless_csv(self, tmp_path_factory):
        path = tmp_path_factory.mktemp("source_columns") / "headerless.csv"
        path.write_text("ACME,42\nGLOBEX,7\n")
        return path

    @pytest.fixture(scope="class")
    def models(self, headerless_csv):
        return {
            "schema.yml": schema_yml.format(csv_path=headerless_csv),
            "allowance_model.sql": model_sql,
        }

    def test_load_reads_declared_columns(self, project):
        results = run_dbt()
        assert len(results) == 1

        res = project.run_sql(
            "select allow_company, allow_pointer from allowance order by 1", fetch="all"
        )
        # allow_pointer is declared as text, so 42 stays a string instead of being
        # read as an integer.
        assert res == [("ACME", "42"), ("GLOBEX", "7")]
