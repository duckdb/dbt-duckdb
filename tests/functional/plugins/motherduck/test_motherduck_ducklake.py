import unittest
from argparse import Namespace
from unittest import mock

from dbt.flags import set_from_args
from dbt.adapters.duckdb import DuckDBAdapter
from dbt.adapters.duckdb.relation import DuckDBRelation
from tests.unit.utils import config_from_parts_or_dicts


class TestMotherduckDucklakeDetection(unittest.TestCase):
    def setUp(self):
        set_from_args(Namespace(STRICT_MODE=True), {})

        # Use a MotherDuck path to align with plugin context, but we won't actually connect
        self.base_profile_cfg = {
            "outputs": {
                "test": {
                    "type": "duckdb",
                    "path": "md:my_db",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.project_cfg = project_cfg
        self.mock_mp_context = mock.MagicMock()

    def _get_adapter(self, profile_cfg):
        config = config_from_parts_or_dicts(self.project_cfg, profile_cfg, cli_vars={})
        return DuckDBAdapter(config, self.mock_mp_context)

    def test_is_ducklake_with_managed_list(self):
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["ducklake_managed_dbs"] = ["lk_db"]

        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="lk_db", schema="main", identifier="t")

        assert adapter.is_ducklake(relation) is True

    def test_is_ducklake_with_managed_list_multiple(self):
        profile_cfg = self.base_profile_cfg.copy()
        profile_cfg["outputs"]["test"]["ducklake_managed_dbs"] = ["lk_db2", "another_db"]

        adapter = self._get_adapter(profile_cfg)
        relation = DuckDBRelation.create(database="lk_db2", schema="main", identifier="t2")

        assert adapter.is_ducklake(relation) is True


