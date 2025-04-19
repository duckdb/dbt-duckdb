import os
import pytest
from dbt.tests.util import run_dbt, relation_from_name

basic_model_sql = """
select range from range(3)
"""


class TestHooks:
    """
    External models should load in dependencies when they exist.

    We test that after materializing upstream and downstream models, we can
    materialize the downstream model by itself, even if we are using an
    in-memory database.
    """

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, tmp_path_factory):
        extroot = str(tmp_path_factory.getbasetemp() / "rematerialize")
        os.mkdir(extroot)
        dbt_profile_target["external_root"] = extroot
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": "select 1;", "transaction": False}]},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": basic_model_sql,
        }

    def test_run(self, project):
        run_dbt(["run"])

        relation = relation_from_name(project.adapter, "basic_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 3
