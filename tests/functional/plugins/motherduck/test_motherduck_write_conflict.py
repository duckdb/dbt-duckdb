import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import DbtRuntimeError


incremental_model_1_sql = """
{{ config(materialized='incremental') }}

select 
    generate_series as id,
    'model_1_data_' || generate_series::varchar as data,
    current_timestamp as created_at
from generate_series(1, 100)

{% if is_incremental() %}
    where generate_series > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
"""

incremental_model_2_sql = """
{{ config(materialized='incremental') }}

select 
    generate_series as id,
    'model_2_data_' || generate_series::varchar as data,
    current_timestamp as created_at
from generate_series(1, 50)

{% if is_incremental() %}
    where generate_series > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
"""


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDWriteConflict:
    """Test to reproduce the write-write conflict with multiple models trying to create the dbt_temp schema concurrently."""

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        """Configure with 2 threads to trigger write conflict."""
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": "test_write_conflict.duckdb",
                        "attach": [
                            {
                                "path": "md:",
                            }  # Attach MotherDuck
                        ],
                        "threads": 2,  # Enable threading to trigger conflict
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model_1.sql": incremental_model_1_sql,
            "incremental_model_2.sql": incremental_model_2_sql,
        }

    def test_write_conflict_on_second_run(self, project):
        """
        Test that reproduces the write-write conflict:
        1. First run always succeeds (initializes both incremental models)
        2. Second run, which is the first true incremental run, should succeed, 
            while it previously failed with a write-write conflict due to
            both models trying to create the dbt_temp schema simultaneously.
        """
        results = run_dbt(expect_pass=True)

        res1 = project.run_sql("SELECT count(*) FROM incremental_model_1", fetch="one")
        assert res1[0] == 100

        res2 = project.run_sql("SELECT count(*) FROM incremental_model_2", fetch="one")
        assert res2[0] == 50

        run_dbt(expect_pass=True)
