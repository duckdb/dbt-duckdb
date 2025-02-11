import os
from uuid import uuid4
import boto3
import pytest
from dbt.tests.adapter.basic.files import (
    base_table_sql,
    model_base,
    schema_base_yml,
    seeds_base_csv,
)
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)

config_materialized_default = """
  {{ config(materialized="external") }}
"""

config_materialized_csv = """
  {{ config(materialized="external", format="csv") }}
"""

config_materialized_parquet_location = """
  {{ config(materialized="external", location="{{ adapter.external_root() }}/test.parquet") }}
"""

config_materialized_csv_location_delim = """
  {{ config(materialized="external", location="{{ adapter.external_root() }}/test_delim.csv", delimiter="|") }}
"""

config_json = """
  {{ config(materialized="external", location="{{ adapter.external_root() }}/test.json") }}
"""

default_external_sql = config_materialized_default + model_base
csv_external_sql = config_materialized_csv + model_base
parquet_table_location_sql = config_materialized_parquet_location + model_base
csv_location_delim_sql = config_materialized_csv_location_delim + model_base
json_sql = config_json + model_base
make_empty = " where id > 10"

DEST_LOCAL = "local"
DEST_S3 = "s3"
BUCKET_NAME = "md-ecosystem-public"

class BaseExternalMaterializations:
    @pytest.fixture(scope="class")
    def models(self, empty):
        return {
            "table_model.sql": base_table_sql + (make_empty if empty else ""),
            "table_default.sql": default_external_sql + (make_empty if empty else ""),
            "table_csv.sql": csv_external_sql + (make_empty if empty else ""),
            "table_parquet_location.sql": parquet_table_location_sql + (make_empty if empty else ""),
            "table_csv_location_delim.sql": csv_location_delim_sql + (make_empty if empty else ""),
            "table_json.sql": json_sql + (make_empty if empty else ""),
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def dest(self):
        return DEST_LOCAL

    @pytest.fixture(scope="class")
    def empty(self):
        return False

    @pytest.fixture(scope="class")
    def extroot(self, tmp_path_factory, dest):
        if dest == DEST_LOCAL:
            extroot = str(tmp_path_factory.getbasetemp() / "external")
            if not os.path.exists(extroot):
                os.mkdir(extroot)

        elif dest == DEST_S3:
            s3_path = os.path.join("dbt-duckdb", "test_external", f"pytest-{uuid4()}")
            extroot = os.path.join("s3://", BUCKET_NAME, s3_path)

        yield extroot

        if dest == DEST_S3:
            session = boto3.Session(
                aws_access_key_id=os.getenv("S3_MD_ORG_KEY"),
                aws_secret_access_key=os.getenv("S3_MD_ORG_SECRET"),
            )
            s3 = session.resource('s3')
            bucket = s3.Bucket('md-ecosystem-public')
            bucket.objects.filter(Prefix=s3_path).delete()

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, profile_type, dbt_profile_target, extroot):
        dbt_profile_target["external_root"] = extroot
        dbt_profile_target["secrets"] = [
            {
                "type": DEST_S3,
                "region": os.getenv("S3_MD_ORG_REGION"),
                "key_id": os.getenv("S3_MD_ORG_KEY"),
                "secret": os.getenv("S3_MD_ORG_SECRET"),
            }
        ]
        if profile_type == "md":
            dbt_profile_target["secrets"][0]["persistent"] = True
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_base(self, project, empty):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 6

        # names exist in result nodes
        check_result_nodes_by_name(
            results,
            [
                "table_model",
                "table_default",
                "table_csv",
                "table_parquet_location",
                "table_csv_location_delim",
                "table_json",
            ],
        )

        # check relation types
        expected = {
            "base": "table",
            "table_model": "table",
            "table_default": "view",
            "table_parquet_location": "view",
            "table_csv": "view",
            "table_csv_location_delim": "view",
            "table_json": "view",
        }
        if empty:
            # if simulating an empty table, result will be empty
            expected.pop("base")
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        models = [
            "base",
            "table_default",
            "table_parquet_location",
            "table_model",
            "table_csv",
            "table_csv_location_delim",
            "table_json",
        ]
        if empty:
            # if simulating an empty table, result will be empty, so don't compare to base table
            models.pop(0)
        check_relations_equal(
            project.adapter,
            models,
        )

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 7
        assert len(catalog.sources) == 1


@pytest.mark.skip_profile("buenavista")
class TestExternalMaterializations(BaseExternalMaterializations):
    pass


@pytest.mark.skip_profile("buenavista")
class TestExternalMaterializationsLocalEmpty(BaseExternalMaterializations):
    @pytest.fixture(scope="class")
    def dest(self):
        return DEST_LOCAL
    
    @pytest.fixture(scope="class")
    def empty(self):
        return True


@pytest.mark.with_s3_creds
@pytest.mark.skip_profile("buenavista")
class TestExternalMaterializationsS3(BaseExternalMaterializations):
    @pytest.fixture(scope="class")
    def dest(self):
        return DEST_S3
    
    @pytest.fixture(scope="class")
    def empty(self):
        return False


@pytest.mark.with_s3_creds
@pytest.mark.skip_profile("buenavista")
class TestExternalMaterializationsS3Empty(BaseExternalMaterializations):
    @pytest.fixture(scope="class")
    def dest(self):
        return DEST_S3
    
    @pytest.fixture(scope="class")
    def empty(self):
        return True
