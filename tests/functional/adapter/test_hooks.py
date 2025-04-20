import uuid
import pytest
from dbt.tests.util import run_dbt, relation_from_name

basic_model_sql = """
select range from range(3)
"""

test_table = f"test_table_{str(uuid.uuid1()).replace('-', '_')}"

post_hook_sql = f"create table {test_table} as select 1;"


class TestPostHook:
    """
    Post hook should run inside txn
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": post_hook_sql}]},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": basic_model_sql,
        }

    def test_run(self, project):
        run_dbt(["run"])

        # check that the model was run
        relation = relation_from_name(project.adapter, "basic_model")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 3

        # check that the post hook was run
        result = project.run_sql(
            f"select count(*) as num_rows from {test_table}", fetch="one"
        )
        assert result[0] == 1

        # reset
        project.run_sql(f"drop table {test_table}")


class TestPostHookTransactionFalse(TestPostHook):
    """
    Post hook should run outside txn
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": post_hook_sql, "transaction": False}]},
        }
