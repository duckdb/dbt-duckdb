"""
Test that parameterized views work as intended.
See README for reasons to use this materialization approach!
"""
import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name
)

seeds__example_seed_csv = """a,b,c
1,2,3
4,5,6
7,8,9
"""

models__example_table = """
{{ config(materialized='table') }}
select * from {{ ref("seed") }}
"""

models__my_parameterized_view = """
{{ config(materialized='parameterized_view') }}
select * from {{ ref("example_table") }}
"""
models__use_parameterized_view = """
{{ config(materialized='table') }}
select * from {{ ref("my_parameterized_view") }}()
"""

models__my_parameterized_view_1_param = """
{{ config(materialized='parameterized_view', parameters='where_a') }}
select * from {{ ref("example_table") }} 
where a = where_a
"""
models__use_parameterized_view_1_param = """
{{ config(materialized='table') }}
select * from {{ ref("my_parameterized_view_1_param") }}(4)
"""

models__my_parameterized_view_1_param_with_comma = """
{{ config(materialized='parameterized_view', parameters='where_a, where_b') }}
select * from {{ ref("example_table") }} 
where 1=1
    and a = where_a 
    and b = where_b
"""
models__use_parameterized_view_1_param_with_comma = """
{{ config(materialized='table') }}
select * from {{ ref("my_parameterized_view_1_param_with_comma") }}(4, 5)
"""

models__my_parameterized_view_2_params = """
{{ config(materialized='parameterized_view', parameters=['where_a', 'where_b']) }}
select * from {{ ref("example_table") }} 
where 1=1
    and a = where_a 
    and b = where_b
"""
models__use_parameterized_view_2_params = """
{{ config(materialized='table') }}
select * from {{ ref("my_parameterized_view_2_params") }}(4, 5)
"""

@pytest.mark.skip_profile("buenavista")
class TestParameterizedView:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seeds__example_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "example_table.sql": models__example_table,
            "my_parameterized_view.sql": models__my_parameterized_view,
            "use_parameterized_view.sql": models__use_parameterized_view,
            "my_parameterized_view_1_param.sql": models__my_parameterized_view_1_param,
            "use_parameterized_view_1_param.sql": models__use_parameterized_view_1_param,
            "my_parameterized_view_1_param_with_comma.sql": models__my_parameterized_view_1_param_with_comma,
            "use_parameterized_view_1_param_with_comma.sql": models__use_parameterized_view_1_param_with_comma,
            "my_parameterized_view_2_params.sql": models__my_parameterized_view_2_params,
            "use_parameterized_view_2_params.sql": models__use_parameterized_view_2_params,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["seed"])

        results = run_dbt(["run"])
        assert len(results) == 9
        check_result_nodes_by_name(results, [
            "example_table",
            "my_parameterized_view",
            "use_parameterized_view",
            "my_parameterized_view_1_param",
            "use_parameterized_view_1_param",
            "my_parameterized_view_1_param_with_comma",
            "use_parameterized_view_1_param_with_comma",
            "my_parameterized_view_2_params",
            "use_parameterized_view_2_params",
        ])
