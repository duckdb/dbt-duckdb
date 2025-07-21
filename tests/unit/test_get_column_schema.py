import unittest
from argparse import Namespace
from unittest import mock

from dbt.flags import set_from_args
from dbt.adapters.duckdb import DuckDBAdapter
from tests.unit.utils import config_from_parts_or_dicts


class TestDuckDBAdapterGetColumnSchemaFromQuery(unittest.TestCase):
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
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg, cli_vars={})
        self.mock_mp_context = mock.MagicMock()
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = DuckDBAdapter(self.config, self.mock_mp_context)
        return self._adapter

    def test_get_column_schema_from_query_with_struct(self):
        """Test get_column_schema_from_query flattens struct columns."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("id", "INTEGER"),
            ("user_data", "STRUCT(name VARCHAR, age INTEGER)")
        ]
        
        with mock.patch.object(self.adapter.connections, 'add_select_query', return_value=(None, mock_cursor)):
            result = self.adapter.get_column_schema_from_query("SELECT * FROM test_table")
            
            # Verify result contains flattened columns (1 simple + 2 from struct)
            self.assertEqual(len(result), 3)
            self.assertEqual(result[0].column, "id")
            self.assertEqual(result[1].column, "user_data.name")
            self.assertEqual(result[2].column, "user_data.age")
