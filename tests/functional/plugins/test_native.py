import os

import pytest

from dbt.tests.adapter.basic.files import (
    base_table_sql,
    model_base,
    schema_base_yml,
    seeds_base_csv,
)
from dbt.tests.util import (
    run_dbt,
)

default_parquet = """
  {{ config(materialized="external_new") }}
  SELECT 1 as a 
"""
upstream_default_parquet = """
  {{ config(materialized="table") }}
  SELECT * from {{ref("default_parquet")}}
"""

partition_model_parquet = """
  {{ config(
        materialized="external_new",
        options =  {
            "partition_by": "a"
        }
        ) 
  }}
  SELECT 1 as a 
"""
upstream_partition_model_parquet = """
  {{ config(materialized="table") }}
  SELECT * from {{ref("partition_model_parquet")}}
"""

default_csv= """
  {{ config(materialized="external_new", format="csv", delimiter="|" ) }}
    SELECT * FROM {{ref("base")}}
  """ 

upstream_default_csv = """
  {{ config(materialized="table") }}
  SELECT * from {{ref("default_csv")}}
"""

default_json= """
  {{ config(materialized="external_new", format="json", location="{{ adapter.external_root() }}/test.json" ) }}
    SELECT * FROM {{ref("base")}}
  """ 

upstream_default_json = """
  {{ config(materialized="table") }}
  SELECT * from {{ref("default_json")}}
"""


@pytest.mark.skip_profile("buenavista", "md")
class TestDuckdbtNativelMaterializations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "default_parquet.sql" : default_parquet,
            "upstream_default_parquet.sql" : upstream_default_parquet,
            "partition_model_parquet.sql": partition_model_parquet,
            "upstream_partition_model_parquet.sql": upstream_partition_model_parquet,
            "default_csv.sql": default_csv,
            "upstream_default_csv.sql": upstream_default_csv,
            "default_json.sql": default_json,
            "upstream_default_json.sql": upstream_default_json
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target,tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "external")
        os.mkdir(extroot)
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": "duckdb.dev",
                        "plugins": [
                            {"module": "native"}
                        ],
                        "external_root" : f'{extroot}'
                    }
                },
                "target": "dev",
            }
        }

    def test_base(self, project):
        results = run_dbt(["seed"])
        # run command
        results = run_dbt()
        print(project.project_root)
        print("break point")



