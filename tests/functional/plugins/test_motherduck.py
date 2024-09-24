from urllib.parse import urlparse
import pytest
from unittest import mock
from unittest.mock import Mock
from dbt.tests.util import (
    run_dbt,
)
from dbt.adapters.duckdb.environments import Environment
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.credentials import PluginConfig
from dbt.adapters.duckdb.plugins.motherduck import Plugin
from dbt.adapters.duckdb.__version__ import version as plugin_version
from dbt.artifacts.schemas.results import RunStatus
from dbt.version import __version__

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
                        # make path unique from other tests that don't pass the token via config
                        # to avoid duckdb 1.1.0 caching error (see https://github.com/duckdb/duckdb/pull/13129)
                        "path": dbt_profile_target.get("path", ":memory:") + "?user=test_motherduck",
                        "plugins": plugins,
                    }
                },
                "target": "dev",
            }
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

    @pytest.fixture(scope="class")
    def models(self, md_sql):
        return {
            "md_table.sql": md_sql,
            "random_logs_test.sql": random_logs_sql,
            "summary_of_logs_test.sql": summary_of_logs_sql,
            "python_pyarrow_table_model.py": python_pyarrow_table_model,
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
        project.run_sql("DROP TABLE python_pyarrow_table_model")

    def test_motherduck(self, project):
        run_dbt()
        res = project.run_sql("SELECT * FROM md_table", fetch="one")
        assert res == (1, "foo")
        res = project.run_sql("SELECT * FROM python_pyarrow_table_model", fetch="all")
        assert res == [(1,), (2,), (3,)]

    def test_incremental(self, project):
        run_dbt()
        res = project.run_sql("SELECT count(*) FROM summary_of_logs_test", fetch="one")
        assert res == (70,)

        run_dbt()
        res = project.run_sql("SELECT count(*) FROM summary_of_logs_test", fetch="one")
        assert res == (105,)

        res = project.run_sql("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'test'", fetch="all")
        assert "dbt_temp_test" in [_r for (_r,) in res]

    def test_incremental_temp_table_exists(self, project):
        project.run_sql('create or replace table test.dbt_temp_test.summary_of_logs_test as (select 1 from generate_series(1,10) g(x))')
        run_dbt()
        res = project.run_sql("SELECT count(*) FROM summary_of_logs_test", fetch="one")
        assert res == (70,)


@pytest.fixture
def mock_plugin_config():
    return {"token": "quack"}


@pytest.fixture
def mock_creds(dbt_profile_target, mock_plugin_config):
    plugin_config = PluginConfig(module="motherduck", config=mock_plugin_config)
    if "md:" in dbt_profile_target["path"]:
        return DuckDBCredentials(path=dbt_profile_target["path"], plugins=[plugin_config])
    return DuckDBCredentials(path=dbt_profile_target["path"])


@pytest.fixture
def mock_plugins(mock_creds, mock_plugin_config):
    plugins = {}
    if mock_creds.is_motherduck:
        plugins["motherduck"] = Plugin.create("motherduck", config=mock_plugin_config)
    return plugins


def test_motherduck_user_agent(dbt_profile_target, mock_plugins, mock_creds):
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
            mock_connect.assert_called_with(dbt_profile_target["path"], read_only=False, config = {})
