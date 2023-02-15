import os

import pytest
import yaml
from dbt.tests.util import run_dbt

sources_schema_yml = """version: 2
sources:
  - name: external_source
    meta:
      external_location: "/tmp/{name}.csv"
    tables:
      - name: seeds_source
        description: "A source table"
        columns:
          - name: id
            description: "An id"
            tests:
              - unique
              - not_null
      - name: seeds_ost
        identifier: "seeds_other_source_table"
        meta:
          external_location: "read_csv_auto('/tmp/{identifier}.csv')"
"""

models_source_model_sql = """select * from {{ source('external_source', 'seeds_source') }}
"""

models_multi_source_model_sql = """select * from {{ source('external_source', 'seeds_source') }}
  inner join {{ source('external_source', 'seeds_ost') }} USING (id)
"""


class TestExternalSources:
    @pytest.fixture(scope="class", autouse=True)
    def setEnvVars(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"

        yield

        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources_schema_yml,
            "source_model.sql": models_source_model_sql,
            "multi_source_model.sql": models_multi_source_model_sql,
        }

    def run_dbt_with_vars(self, project, cmd, *args, **kwargs):
        vars_dict = {
            "test_run_schema": project.test_schema,
        }
        cmd.extend(["--vars", yaml.safe_dump(vars_dict)])
        return run_dbt(cmd, *args, **kwargs)

    @pytest.fixture(scope="class")
    def seeds_source_file(self):
        with open("/tmp/seeds_source.csv", "w") as f:
            f.write("id,a,b\n1,2,3\n4,5,6\n7,8,9")
        yield
        os.unlink("/tmp/seeds_source.csv")

    @pytest.fixture(scope="class")
    def ost_file(self):
        with open("/tmp/seeds_other_source_table.csv", "w") as f:
            f.write("id,c,d\n1,2,3\n4,5,6\n7,8,9")
        yield
        os.unlink("/tmp/seeds_other_source_table.csv")

    def test_external_sources(self, seeds_source_file, ost_file, project):
        results = self.run_dbt_with_vars(project, ["run"])
        assert len(results) == 2
        test_results = self.run_dbt_with_vars(project, ["test"])
        assert len(test_results) == 2
