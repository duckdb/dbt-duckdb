from pathlib import Path

import pytest
from dbt.tests.util import run_dbt


LAKE_ALIAS = "typed_lake"

models__table_model = f"""
{{{{ config(materialized='table', database='{LAKE_ALIAS}') }}}}

select 1 as id
"""


@pytest.mark.requires_ducklake
@pytest.mark.skip_profile("buenavista", "md")
class TestDucklakeAttachType:
    """A DuckLake catalog attached with `type: ducklake` instead of the `ducklake:`
    path prefix must still get the DuckLake-specific DDL."""

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, tmp_path_factory):
        root = Path(tmp_path_factory.mktemp("typed_ducklake"))
        data_path = root / "data"
        data_path.mkdir()

        target = dict(dbt_profile_target)
        target["path"] = target.get("path", ":memory:")
        target["attach"] = [
            {
                "path": str(root / "metadata.ducklake"),
                "alias": LAKE_ALIAS,
                "type": "ducklake",
                "options": {"data_path": str(data_path)},
            }
        ]
        return {"test": {"outputs": {"dev": target}, "target": "dev"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"table_model.sql": models__table_model}

    def test_table_model_can_be_rebuilt(self, project):
        run_dbt(["run"])
        # Rebuilding swaps the new table in and drops the backup, which DuckLake
        # only accepts without CASCADE
        result = run_dbt(["run"])
        node = result.results[0].node

        relation = project.adapter.Relation.create(
            database=node.database, schema=node.schema, identifier=node.alias
        )
        assert node.database == LAKE_ALIAS
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 1
