import tempfile
from pathlib import Path

import pytest
from dbt.tests.util import (
    run_dbt, )

from tests.functional.plugins.utils import get_table_row_count

ref1 = """
select 2 as a, 'test' as b 
"""


def delta_table_sql(location: str) -> str:
    return f"""
    {{{{ config(
        materialized='table',
        plugin = 'delta',
        location = '{location}',
        mode = 'merge',
        unique_key = 'a'

    ) }}}}
    select * from {{{{ref('ref1')}}}}
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
    @pytest.fixture(scope="class")
    def delta_test_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = Path(tmpdir) / "test_delta_table"
            yield table_path

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

            "delta_table.sql": delta_table_sql(str(delta_test_table)),
            "ref1.sql": ref1
        }

    def test_plugins(self, project):
        results = run_dbt()
        assert len(results) == 2

        delta_table_row_count = get_table_row_count(project, "main.delta_table")
        assert delta_table_row_count == 1
