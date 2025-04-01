"""
Test that table functions work as intended.
See README for reasons to use this materialization approach!
"""
import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name
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

models__my_table_function = """
{{ config(materialized='table_function') }}
select * from {{ ref("example_table") }}
"""

models__use_table_function = """
{{ config(materialized='table') }}
select * from {{ ref("my_table_function") }}()
"""

models__my_table_function_1_param = """
{{ config(materialized='table_function', parameters='where_a') }}
select * from {{ ref("example_table") }} 
where a = where_a
"""

models__use_table_function_1_param = """
{{ config(materialized='table') }}
select * from {{ ref("my_table_function_1_param") }}(4)
"""

models__my_table_function_1_param_with_comma = """
{{ config(materialized='table_function', parameters='where_a, where_b') }}
select * from {{ ref("example_table") }} 
where 1=1
    and a = where_a 
    and b = where_b
"""

models__use_table_function_1_param_with_comma = """
{{ config(materialized='table') }}
select * from {{ ref("my_table_function_1_param_with_comma") }}(4, 5)
"""

models__my_table_function_2_params = """
{{ config(materialized='table_function', parameters=['where_a', 'where_b']) }}
select * from {{ ref("example_table") }} 
where 1=1
    and a = where_a 
    and b = where_b
"""

models__use_table_function_2_params = """
{{ config(materialized='table') }}
select * from {{ ref("my_table_function_2_params") }}(4, 5)
"""

# To test that the table function will work smoothly even if a column is added:
#   Create an example_table
#   create a table_function that is select * from example_table
#   Persist the output of that table function to a table
#   Alter the table to add a column
#   Persist the output of that table function to a new table (should include the new column)
# Note this will not recreate the table_function (which would have been needed with a view)
models__use_table_function_after_adding_column = """
-- depends_on: {{ ref('use_table_function') }}
{{ config(materialized='table') }}
{% set alter_table_query %}
alter table {{ ref("example_table") }} add column d integer default 42
{% endset %}

{% set results = run_query(alter_table_query) %}
select * from {{ ref("my_table_function") }}()
"""



@pytest.mark.skip_profile("buenavista")
class TestTableFunction:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seeds__example_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "example_table.sql": models__example_table,
            "my_table_function.sql": models__my_table_function,
            "use_table_function.sql": models__use_table_function,
            "my_table_function_1_param.sql": models__my_table_function_1_param,
            "use_table_function_1_param.sql": models__use_table_function_1_param,
            "my_table_function_1_param_with_comma.sql": models__my_table_function_1_param_with_comma,
            "use_table_function_1_param_with_comma.sql": models__use_table_function_1_param_with_comma,
            "my_table_function_2_params.sql": models__my_table_function_2_params,
            "use_table_function_2_params.sql": models__use_table_function_2_params,
            "use_table_function_after_adding_column.sql": models__use_table_function_after_adding_column,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["seed"])

        results = run_dbt(["run"])
        assert len(results) == 10
        check_result_nodes_by_name(results, [
            "example_table",
            "my_table_function",
            "use_table_function",
            "my_table_function_1_param",
            "use_table_function_1_param",
            "my_table_function_1_param_with_comma",
            "use_table_function_1_param_with_comma",
            "my_table_function_2_params",
            "use_table_function_2_params",
            "use_table_function_after_adding_column"
        ])

        relation_pre_alter = relation_from_name(project.adapter, "use_table_function")
        result_pre_alter = project.run_sql(f"describe {relation_pre_alter}", fetch="all")
        column_names_pre_alter = [row[0] for row in result_pre_alter]
        assert column_names_pre_alter == ['a', 'b', 'c']

        relation_post_alter = relation_from_name(project.adapter, "use_table_function_after_adding_column")
        result_post_alter = project.run_sql(f"describe {relation_post_alter}", fetch="all")
        column_names_post_alter = [row[0] for row in result_post_alter]
        assert column_names_post_alter == ['a', 'b', 'c', 'd']
