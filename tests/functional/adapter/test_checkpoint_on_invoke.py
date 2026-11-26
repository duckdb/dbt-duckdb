import os

import pytest
from dbt.tests.util import run_dbt

model_sql = """
{{ config(materialized='table') }}
select 1 as id, 'main' as note
"""


@pytest.mark.skip_profile("memory", "buenavista", "md")
class TestCheckpointOnInvoke:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    def test_no_wal_after_two_invocations(self, project, dbt_profile_target, database_type):
        # The checkpoint only covers native DuckDB files, so a DuckLake
        # catalog has no .wal file to check for.
        if database_type == "ducklake":
            pytest.skip("checkpoint only covers native DuckDB files")
        results = run_dbt()
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1
        # Both invocations ran in this process against the same shared
        # connection (keep_open). The checkpoint at the end of each
        # invocation must have flushed the WAL, so no .wal file is left.
        db_path = dbt_profile_target["path"]
        assert not os.path.exists(db_path + ".wal")
