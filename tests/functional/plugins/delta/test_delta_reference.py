import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

ref1 = """
SELECT 1 as a, 'test' as b 
UNION ALL
SELECT 2 as a, 'test2' as b 
UNION ALL
SELECT 3 as a, 'test3' as b 
"""

delta1 = """
    {{{{ config(
        materialized='external',
        mode = 'append',
        plugin = 'delta',
        location = '{root_path}/delta1',
    ) }}}}

    
    select * from {{{{ref('ref1')}}}}

    {{% if var('first_run') == 'true' %}}
        WHERE a < 2
    {{% else %}}
        WHERE a >= (SELECT max(a) from {{{{ this }}}})
    {{% endif %}}
     
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
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
    def project_config_update(self):
        return {
            "name": "base",
            "on-run-start": ["{{ register_self_reference_external_models() }}"],
        }

    @pytest.fixture(scope="class")
    def models(self, project_root):
        return {
            "delta1.sql": delta1.format(root_path=project_root),
            "ref1.sql": ref1,
        }

    def test_plugins(self, project):
        # This doesnt work because we need some kind of incremental thinking
        # i made it register on the begining but if the table doesnt exists by the first run it can't register
        # We have to see how to do it
        # I tried to simulate the incremental model with the variables which works

        results = run_dbt(
            [
                "run",
                "--select",
                "ref1 delta1",
                "--vars",
                "{'first_run': 'true'}",
                "-d",
            ]
        )

        res = project.run_sql("SELECT * FROM 'delta1'", fetch="all")

        results = run_dbt(
            [
                "run",
                "--select",
                "ref1 delta1",
                "--vars",
                "{'first_run': 'false'}",
                "-d",
            ]
        )

        res = project.run_sql("SELECT * FROM 'delta1'", fetch="all")
        assert res == [(1, "test"), (3, "test3"), (2, "test2"), (1, "test")]
        print("break point")
