import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

ref1 = """
select 2 as a, 'test' as b 
"""

delta1 = """
    {{{{ config(
        materialized='external',
        plugin = 'delta',
        location = '{root_path}/delta1',
    ) }}}}
    select * from {{{{ref('ref1')}}}}
"""

upstream_delta1 = """
    {{{{ config(
        materialized='external',
        plugin = 'delta',
        location = '{root_path}/upstream_delta1',
    ) }}}}
    select * from {{{{ref('delta1')}}}}
"""

upstream_delta1 = """
    {{{{ config(
        materialized='external',
        plugin = 'delta',
        location = '{root_path}/upstream_delta1',
    ) }}}}
    select * from {{{{ref('delta1')}}}}
"""

upstream_duckdb1 = """
{{ config(
        materialized='table',
    ) }}
select * from {{ref('upstream_delta1')}}
"""

delta2 = """
    {{{{ config(
        materialized='external',
        plugin = 'delta',
        mode = "append",
        location = '{root_path}/delta2',
    ) }}}}
    select * from {{{{ref('ref1')}}}}
"""

upstream_delta2 = """
    {{{{ config(
        materialized='external',
        plugin = 'delta',
        location = '{root_path}/upstream_delta2',
    ) }}}}
    select * from {{{{ref('delta2')}}}}
"""


ref2 = """
select 2 as a, 'test1' as b 
UNION ALL
select 3 as a, 'test2' as b 
"""

delta3 = """
    {{{{ config(
        materialized='external',
        plugin = 'delta',
        location = '{root_path}/delta1',
        mode = 'merge',
        unique_key = 'a'
    ) }}}}
    select * from {{{{ref('ref2')}}}}
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
    def models(self, project_root):
        return {
            "delta1.sql": delta1.format(root_path=project_root),
            "upstream_delta1.sql": upstream_delta1.format(root_path=project_root),
            "upstream_duckdb1.sql": upstream_duckdb1,
            "delta2.sql": delta2.format(root_path=project_root),
            "upstream_delta2.sql": upstream_delta2.format(root_path=project_root),
            "ref1.sql": ref1,
            "ref2.sql": ref2,
            "delta3.sql": delta3.format(root_path=project_root),
        }

    def test_plugins(self, project):
        results = run_dbt(
            ["run", "--select", "ref1 delta1 upstream_delta1 upstream_duckdb1"]
        )

        # overwrite with upstream model
        check_relations_equal(
            project.adapter,
            [
                "ref1",
                "upstream_duckdb1",
            ],
        )
        res = project.run_sql("SELECT count(1) FROM 'upstream_delta1'", fetch="one")
        assert res[0] == 1

        # append with upstream model

        results = run_dbt(["run", "--select", "ref1 delta2"])

        results = run_dbt(["run", "--select", "ref1 delta2 upstream_delta2"])

        res = project.run_sql("SELECT * FROM 'delta2'", fetch="all")
        assert len(res) == 2

        # merge
        # reuse path from delta1
        results = run_dbt(["run", "--select", "ref2 delta3"])

        res = project.run_sql("SELECT * FROM 'delta3'", fetch="all")
        assert res == [(3, "test2"), (2, "test1")]
