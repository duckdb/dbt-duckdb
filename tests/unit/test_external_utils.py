import unittest
from argparse import Namespace
from unittest import mock

from dbt.flags import set_from_args
from dbt.adapters.duckdb import DuckDBAdapter
from tests.unit.utils import config_from_parts_or_dicts

class TestExternalUtils(unittest.TestCase):
    def setUp(self):
        set_from_args(Namespace(STRICT_MODE=True), {})

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "duckdb",
                    "path": ":memory:",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg, cli_vars={})
        self._adapter = None

    @property
    def adapter(self):
        self.mock_mp_context = mock.MagicMock()
        if self._adapter is None:
            self._adapter = DuckDBAdapter(self.config, self.mock_mp_context)
        return self._adapter

    def test_external_write_options(self):
        data = [
            ("/tmp/test.csv", {}, "format csv, header 1"),
            ("./foo.parquet", {"codec": "zstd"}, "codec zstd, format parquet"),
            ("bar", {"delimiter": "|", "header": "0"}, "delimiter '|', header 0, format csv"),
            ("a.parquet", {"partition_by": "ds"}, "partition_by ds, format parquet"),
            ("b.csv", {"partition_by": "ds,category"}, "partition_by (ds,category), format csv, header 1"),
            ("/path/to/c.csv", {"null": "\\N"}, "null '\\N', format csv, header 1")
        ]

        for (loc, opts, expected) in data:
            assert expected == self.adapter.external_write_options(loc, opts)
    

    def test_external_read_location(self):
        data = [
            ("bar", {"format": "csv", "delimiter": "|", "header": "0"}, "bar"),
            ("/tmp/a", {"partition_by": "ds", "format": "parquet"}, "/tmp/a/*/*.parquet"),
            ("b", {"partition_by": "ds,category"}, "b/*/*/*.parquet"),
        ]
        for (loc, opts, expected) in data:
            assert expected == self.adapter.external_read_location(loc, opts)