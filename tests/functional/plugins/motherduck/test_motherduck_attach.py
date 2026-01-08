import pytest
from dbt.tests.util import (
    run_dbt,
)

random_logs_sql = """
{{ config(materialized='table', meta=dict(temp_schema_name='dbt_temp_test')) }}

select 
  uuid()::varchar as log_id,
 '2023-10-01'::timestamp + interval 1 minute * (random() * 20000)::int as dt ,
(random() * 4)::int64 as user_id
from generate_series(1, 10000) g(x)
"""

summary_of_logs_sql = """
{{
    config(
        materialized='incremental',
        meta=dict(temp_schema_name='dbt_temp_test'),
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

python_pyarrow_table_model = """
import pyarrow as pa

def model(dbt, con):
    return pa.Table.from_pydict({"a": [1,2,3]})
"""

@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDPluginAttach:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, test_database_name):
        md_config = {"token": dbt_profile_target.get("token")}
        plugins = [{"module": "motherduck", "config": md_config}]
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": ":memory:",
                        "plugins": plugins,
                        "attach": [
                            {
                                "path": f"md:{test_database_name}",
                                "type": "motherduck"
                            }
                        ]
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, md_sql):
        return {
            "md_table.sql": md_sql,
            "random_logs_test.sql": random_logs_sql,
            "summary_of_logs_test.sql": summary_of_logs_sql,
            "python_pyarrow_table_model.py": python_pyarrow_table_model,
        }

    @pytest.fixture(scope="class")
    def md_sql(self, test_database_name):
        # Reads from a MD database in my test account in the cloud
        return f"""
            select * FROM {test_database_name}.main.plugin_table
        """
    
    @pytest.fixture(autouse=True)
    def run_dbt_scope(self, project, test_database_name):
        project.run_sql(f"CREATE OR REPLACE TABLE {test_database_name}.plugin_table (i integer, j string)")
        project.run_sql(f"INSERT INTO {test_database_name}.plugin_table (i, j) VALUES (1, 'foo')")
        yield
        project.run_sql("DROP VIEW IF EXISTS md_table")
        project.run_sql("DROP TABLE IF EXISTS random_logs_test")
        project.run_sql("DROP TABLE IF EXISTS summary_of_logs_test")
        project.run_sql(f"DROP TABLE IF EXISTS {test_database_name}.plugin_table")
        project.run_sql("DROP TABLE IF EXISTS python_pyarrow_table_model")

    def test_motherduck(self, project):
        run_dbt(expect_pass=True)


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDPluginAttachWithSettings(TestMDPluginAttach):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, test_database_name):
        md_setting = {"motherduck_token": dbt_profile_target.get("token")}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": ":memory:",
                        "attach": [
                            {
                                "path": f"md:{test_database_name}",
                                "type": "motherduck"
                            }
                        ],
                        "settings": md_setting
                    }
                },
                "target": "dev",
            }
        }


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDPluginAttachWithTokenInPath(TestMDPluginAttach):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, test_database_name):
        token = dbt_profile_target.get("token")
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": ":memory:",
                        "attach": [
                            {
                                "path": f"md:{test_database_name}?motherduck_token={token}&user=1",
                                "type": "motherduck"
                            }
                        ]
                    }
                },
                "target": "dev",
            }
        }
