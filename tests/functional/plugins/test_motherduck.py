from unittest import mock

import pytest
from dbt.tests.util import (
    run_dbt,
)
from dbt.version import __version__

from dbt.adapters.duckdb.__version__ import version as plugin_version
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.credentials import PluginConfig
from dbt.adapters.duckdb.environments import Environment
from dbt.adapters.duckdb.plugins.motherduck import Plugin

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


@pytest.mark.skip_profile("buenavista", "file", "memory", "unity")
class TestMDPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        md_config = {"token": dbt_profile_target.get("token")}
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

        res = project.run_sql("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'test'",
                              fetch="all")
        assert "dbt_temp_test" in [_r for (_r,) in res]

    def test_incremental_temp_table_exists(self, project):
        project.run_sql(
            'create or replace table test.dbt_temp_test.summary_of_logs_test as (select 1 from generate_series(1,10) g(x))')
        run_dbt()
        res = project.run_sql("SELECT count(*) FROM summary_of_logs_test", fetch="one")
        assert res == (70,)

    @pytest.fixture
    def mock_md_plugin(self):
        return Plugin.create("motherduck")

    @pytest.fixture
    def mock_creds(self, dbt_profile_target):
        plugin_config = PluginConfig(module="motherduck", config={"token": "quack"})
        if "md:" in dbt_profile_target["path"]:
            return DuckDBCredentials(path=dbt_profile_target["path"], plugins=[plugin_config])
        return DuckDBCredentials(path=dbt_profile_target["path"])

    @pytest.fixture
    def mock_plugins(self, mock_creds, mock_md_plugin):
        plugins = {}
        if mock_creds.is_motherduck:
            plugins["motherduck"] = mock_md_plugin
        return plugins

    def test_motherduck_user_agent(self, dbt_profile_target, mock_plugins, mock_creds):
        with mock.patch("dbt.adapters.duckdb.environments.duckdb.connect") as mock_connect:
            mock_creds.settings = {"custom_user_agent": "downstream-dep"}
            Environment.initialize_db(mock_creds, plugins=mock_plugins)
            if mock_creds.is_motherduck:
                kwargs = {
                    'read_only': False,
                    'config': {
                        'custom_user_agent': f'dbt/{__version__} dbt-duckdb/{plugin_version} downstream-dep',
                        'motherduck_token': 'quack'
                    }
                }
                mock_connect.assert_called_with(dbt_profile_target["path"], **kwargs)
            else:
                mock_connect.assert_called_with(dbt_profile_target["path"], read_only=False, config={})
