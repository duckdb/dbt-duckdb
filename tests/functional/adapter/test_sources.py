import os

import pytest
from dbt.tests.util import run_dbt

sources_schema_yml = """version: 2
sources:
  - name: external_source
    config:
      external_location: "/tmp/{name}_{extra}.csv"
    tables:
      - name: seeds_source
        description: "A source table"
        config:
          extra: 'something'
        columns:
          - name: id
            description: "An id"
            tests:
              - unique
              - not_null
      - name: seeds_ost
        identifier: "seeds_other_source_table"
        config:
          external_location: "read_csv_auto('/tmp/%(identifier)s.csv')"
          formatter: oldstyle
      - name: seeds_other_source_table
        config:
          external_location: "read_csv_auto('/tmp/${name}.csv')"
          formatter: template
"""

models_source_model_sql = """select * from {{ source('external_source', 'seeds_source') }}
"""

models_multi_source_model_sql = """select s.* from {{ source('external_source', 'seeds_source') }} s
  inner join {{ source('external_source', 'seeds_ost') }} oldstyle USING (id)
  inner join {{ source('external_source', 'seeds_other_source_table') }} tmpl USING (id)
"""


class TestExternalSources:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources_schema_yml,
            "source_model.sql": models_source_model_sql,
            "multi_source_model.sql": models_multi_source_model_sql,
        }

    @pytest.fixture(scope="class")
    def seeds_source_file(self):
        with open("/tmp/seeds_source_something.csv", "w") as f:
            f.write("id,a,b\n1,2,3\n4,5,6\n7,8,9")
        yield
        os.unlink("/tmp/seeds_source_something.csv")

    @pytest.fixture(scope="class")
    def ost_file(self):
        with open("/tmp/seeds_other_source_table.csv", "w") as f:
            f.write("id,c,d\n1,2,3\n4,5,6\n7,8,9")
        yield
        os.unlink("/tmp/seeds_other_source_table.csv")

    def test_external_sources(self, seeds_source_file, ost_file, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 2
