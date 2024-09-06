import tempfile
from pathlib import Path

import pytest
from dbt.tests.util import (
    run_dbt,
)

ref1 = """
select 2 as a, 'test' as b 
"""


def unity_create_table_sql(location: str) -> str:
    return f"""
    {{{{ config(
        materialized='external_table',
        plugin = 'unity',
        location = '{location}'
    ) }}}}
    select * from {{{{ref('ref1')}}}}
"""


def unity_create_table_and_schema_sql(location: str) -> str:
    return f"""
    {{{{ config(
        materialized='external_table',
        plugin = 'unity',
        schema = 'test_schema',
        location = '{location}'
    ) }}}}
    select * from {{{{ref('ref1')}}}}
"""


@pytest.mark.skip_profile("buenavista", "file", "memory", "md")
class TestPlugins:
    @pytest.fixture(scope="class")
    def unity_create_table(self):
        td = tempfile.TemporaryDirectory()
        path = Path(td.name)
        table_path = path / "test_unity_create_table"

        yield table_path

        td.cleanup()

    @pytest.fixture(scope="class")
    def unity_create_table_and_schema(self):
        td = tempfile.TemporaryDirectory()
        path = Path(td.name)
        table_path = path / "test_unity_create_table_and_schema"

        yield table_path

        td.cleanup()

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        plugins = [{"module": "unity"}]
        extensions = dbt_profile_target.get("extensions")
        extensions.extend([{"name": "delta"}])
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": plugins,
                        "extensions": extensions,
                        "secrets": dbt_profile_target.get("secrets"),
                        "attach": dbt_profile_target.get("attach")
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, unity_create_table, unity_create_table_and_schema):
        return {
            "unity_create_table.sql": unity_create_table_sql(str(unity_create_table)),
            "unity_create_table_and_schema.sql": unity_create_table_and_schema_sql(str(unity_create_table_and_schema)),
            "ref1.sql": ref1
        }

    def test_plugins(self, project):
        results = run_dbt()
        assert len(results) == 3
