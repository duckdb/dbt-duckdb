import os
import re

import pytest
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeral,
    BaseEphemeralMulti,
    ephemeral_errors__base__base_copy_sql,
    ephemeral_errors__base__base_sql,
    ephemeral_errors__dependent_sql,
    models_n__ephemeral_level_two_sql,
    models_n__ephemeral_sql,
    models_n__root_view_sql,
    models_n__source_table_sql,
)
from dbt.tests.util import check_relations_equal, run_dbt


class TestEphemeralMulti(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])
        assert os.path.exists("./target/run/test/models/double_dependent.sql")
        with open("./target/run/test/models/double_dependent.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            'create view "memory"."test_test_ephemeral"."double_dependent__dbt_tmp" as ('
            "with __dbt__cte__base as ("
            "select * from test_test_ephemeral.seed"
            "),  __dbt__cte__base_copy as ("
            "select * from __dbt__cte__base"
            ")-- base_copy just pulls from base. Make sure the listed"
            "-- graph of CTEs all share the same dbt_cte__base cte"
            "select * from __dbt__cte__base where gender = 'Male'"
            "union all"
            "select * from __dbt__cte__base_copy where gender = 'Female'"
            ");"
        )
        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralNested(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_level_two.sql": models_n__ephemeral_level_two_sql,
            "root_view.sql": models_n__root_view_sql,
            "ephemeral.sql": models_n__ephemeral_sql,
            "source_table.sql": models_n__source_table_sql,
        }

    def test_ephemeral_nested(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert os.path.exists("./target/run/test/models/root_view.sql")
        with open("./target/run/test/models/root_view.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            'create view "memory"."test_test_ephemeral"."root_view__dbt_tmp" as ('
            "with __dbt__cte__ephemeral_level_two as ("
            'select * from "memory"."test_test_ephemeral"."source_table"'
            "),  __dbt__cte__ephemeral as ("
            "select * from __dbt__cte__ephemeral_level_two"
            ")select * from __dbt__cte__ephemeral"
            ");"
        )

        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralErrorHandling(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": ephemeral_errors__base__base_sql,
                "base_copy.sql": ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Compilation Error" in results[0].message
