import pytest
from pathlib import Path
import pandas as pd
import tempfile

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)
from deltalake.writer import write_deltalake

delta_schema_yml = """

"""

ref1 = """
select 2 as a, 'test' as b 
"""

delta1_sql = """
    {{ config(
        materialized='external_table',
        plugin = 'delta',
        location = '/workspaces/dbt-duckdb/delta_test',
        storage_options = {
            'test' : 'test'
        }

    ) }}
    select * from {{ref('ref1')}} 
"""

delta2_sql = """
    {{ config(
        materialized='external_table',
        plugin = 'delta',
        location = '/workspaces/dbt-duckdb/delta2_test',
        mode = 'merge',
        unique_key = 'a'

    ) }}
    select * from {{ref('ref1')}} 
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
    @pytest.fixture(scope="class")
    def delta_test_table1(self):
        td = tempfile.TemporaryDirectory() 
        path = Path(td.name)
        table_path = path / "test_delta_table1"

        yield table_path

        td.cleanup()

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
    def models(self, delta_test_table1):
        return {
            
            "delta_table2.sql": delta2_sql,
            "ref1.sql": ref1
        }

    def test_plugins(self, project):
        results = run_dbt()
        #assert len(results) == 4

        # check_relations_equal(
        #     project.adapter,
        #     [
        #         "delta_table3",
        #         "delta_table3_expected",
        #     ],
        # )
        # res = project.run_sql("SELECT count(1) FROM 'delta_table3'", fetch="one")
        # assert res[0] == 2
