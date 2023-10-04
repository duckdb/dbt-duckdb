import os
import pytest
import sqlite3
from pathlib import Path
import shutil
import pandas as pd

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)
from deltalake.writer import write_deltalake

delta_schema_yml = """
version: 2
sources:
  - name: delta_source
    schema: main
    meta:
      plugin: delta
    tables:
      - name: table_1
        description: "An delta table"
        meta:
          materialization: "view"
          delta_table_path: "{test_delta_path}"
"""


delta1_sql = """
    {{ config(materialized='table') }}
    select * from {{ source('delta_source', 'table_1') }}
"""

# plugin_sql = """
#     {{ config(materialized='external', plugin='cfp', key='value') }}
#     select foo() as foo
# """

# # Reads from a MD database in my test account in the cloud
# md_sql = """
#     select * FROM plugin_test.main.plugin_table
# """


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
    @pytest.fixture(scope="class")
    def delta_test_table(self):
        path = Path("/tmp/test_delta")
        table_path = path / "test_delta_table"

        df = pd.DataFrame({"x": [1, 2, 3]})
        write_deltalake(table_path, df, mode="overwrite")

        yield table_path

        shutil.rmtree(table_path)

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        plugins = [{"module": "delta"}]
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
    def models(self, delta_test_table):
        return {
            "source_schema.yml": delta_schema_yml.format(
                test_delta_path=delta_test_table
            ),
            "delta_table.sql": delta1_sql,
        }

    def test_plugins(self, project):
        results = run_dbt()
        assert len(results) == 1
