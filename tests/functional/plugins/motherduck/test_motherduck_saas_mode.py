from urllib.parse import urlparse
import pytest
from dbt.tests.util import (
    run_dbt,
)
from dbt.artifacts.schemas.results import RunStatus

from dbt.adapters.duckdb.environments.motherduck import MOTHERDUCK_SAAS_MODE_QUERY

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
class TestMDPluginSaaSMode:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        md_config = {"motherduck_token": dbt_profile_target.get("token"), "motherduck_saas_mode": True}
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:") + "?user=1",
                        "config_options": md_config,
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
    def database_name(self, dbt_profile_target):
        return urlparse(dbt_profile_target["path"]).path
    
    @pytest.fixture(scope="class")
    def md_sql(self, database_name):
        # Reads from a MD database in my test account in the cloud
        return f"""
            select * FROM {database_name}.main.plugin_table
        """
    
    @pytest.fixture(autouse=True)
    def run_dbt_scope(self, project, database_name):
        # CREATE DATABASE does not work with SaaS mode on duckdb 1.0.0
        # This will be fixed in duckdb 1.1.1
        # project.run_sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        project.run_sql(f"CREATE OR REPLACE TABLE {database_name}.plugin_table (i integer, j string)")
        project.run_sql(f"INSERT INTO {database_name}.plugin_table (i, j) VALUES (1, 'foo')")
        yield
        project.run_sql("DROP VIEW md_table")
        project.run_sql("DROP TABLE random_logs_test")
        project.run_sql("DROP TABLE summary_of_logs_test")
        project.run_sql(f"DROP TABLE {database_name}.plugin_table")

    def test_motherduck(self, project):
        (motherduck_saas_mode,) = project.run_sql(MOTHERDUCK_SAAS_MODE_QUERY, fetch="one")
        if str(motherduck_saas_mode).lower() not in ["1", "true"]:
            raise ValueError("SaaS mode was not set")
        result = run_dbt(expect_pass=False)
        expected_msg = "Python models are disabled when MotherDuck SaaS Mode is on."
        assert [_res for _res in result.results if _res.status != RunStatus.Success][0].message == expected_msg


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDPluginSaaSModeViaAttach(TestMDPluginSaaSMode):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        md_config = {
            "token": dbt_profile_target.get("token"),
            "saas_mode": 1
        }
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
                                "path": dbt_profile_target.get("path", ":memory:") + "?user=2",
                                "type": "motherduck"
                            }
                        ]
                    }
                },
                "target": "dev",
            }
        }


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMDPluginSaaSModeViaAttachWithSettings(TestMDPluginSaaSMode):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        md_setting = {
            "motherduck_token": dbt_profile_target.get("token"),
            "motherduck_saas_mode": True
        }
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": ":memory:",
                        "attach": [
                            {
                                "path": dbt_profile_target.get("path", ":memory:") + "?user=3",
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
class TestMDPluginSaaSModeViaAttachWithTokenInPath(TestMDPluginSaaSMode):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        token = dbt_profile_target.get("token")
        qs = f"?motherduck_token={token}&saas_mode=true&user=4"
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": ":memory:",
                        "attach": [
                            {
                                "path": dbt_profile_target.get("path", ":memory:") + qs,
                                "type": "motherduck"
                            }
                        ]
                    }
                },
                "target": "dev",
            }
        }
