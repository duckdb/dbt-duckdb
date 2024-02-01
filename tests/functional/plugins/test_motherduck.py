import pytest
from dbt.tests.util import (
    run_dbt,
)

random_logs_sql = """
{{ config(materialized='table') }}

select 
  uuid()::varchar as log_id,
 '2023-10-01'::timestamp + interval 1 minute * (random() * 20000)::int as dt ,
(random() * 4)::int64 as user_id
from generate_series(1, 10000) g(x)
"""

summary_of_logs_sql = """
{{
    config(
        materialized='incremental'
    )
}}

select dt::date as dt, user_id, count(1) as c
from {{ ref('random_logs_test') }}


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where dt > '2023-10-08'::timestamp

{% endif %}
group by all
"""

@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        md_config = {}
        plugins = [{"module": "motherduck", "config": md_config}]
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
    def database_name(self, dbt_profile_target):
        return dbt_profile_target["path"].replace("md:", "")
    
    @pytest.fixture(scope="class")
    def md_sql(self, database_name):
        # Reads from a MD database in my test account in the cloud
        return f"""
            select * FROM {database_name}.main.plugin_table
        """

    @pytest.fixture(scope="class")
    def models(self, md_sql):
        return {
            "md_table.sql": md_sql,
            "random_logs_test.sql": random_logs_sql,
            "summary_of_logs_test.sql": summary_of_logs_sql,
        }

    @pytest.fixture(autouse=True)
    def run_dbt_scope(self, project, database_name):
        project.run_sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        project.run_sql("CREATE OR REPLACE TABLE plugin_table (i integer, j string)")
        project.run_sql("INSERT INTO plugin_table (i, j) VALUES (1, 'foo')")
        yield
        project.run_sql("DROP VIEW md_table")
        project.run_sql("DROP TABLE random_logs_test")
        project.run_sql("DROP TABLE summary_of_logs_test")
        project.run_sql("DROP TABLE plugin_table")

    def test_motherduck(self, project):
        run_dbt()
        res = project.run_sql("SELECT * FROM md_table", fetch="one")
        assert res == (1, "foo")

    def test_incremental(self, project):
        run_dbt()
        res = project.run_sql("SELECT count(*) FROM summary_of_logs_test", fetch="one")
        assert res == (70,)

        run_dbt()
        res = project.run_sql("SELECT count(*) FROM summary_of_logs_test", fetch="one")
        assert res == (105,)
