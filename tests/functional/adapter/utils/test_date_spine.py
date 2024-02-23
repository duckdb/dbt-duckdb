import pytest

from dbt.tests.util import (
    run_dbt,
)

my_date_spine_model = """
{{
    config(
        materialized = 'table',
    )
}}

with days as (

    {{
        dbt_utils.date_spine(
            'day',
            "'2024-01-01'::timestamp",
            dbt.dateadd('day', 1, "'2024-02-01'::timestamp"),
        )
    }}

),

final as (
    select cast(date_day as date) as date_day
    from days
)

select * from final
"""

class TestDateSpine:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "date_spine.sql": my_date_spine_model,
        }
    
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "dbt-labs/dbt_utils", "version": "1.1.1"}]}

    def test_date_spine(self, project):

        # install dbt_utils
        run_dbt(["deps"])
        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1
