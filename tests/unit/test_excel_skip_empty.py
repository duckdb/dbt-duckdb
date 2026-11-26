from unittest.mock import Mock

import pytest

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig, TargetLocation

pandas = pytest.importorskip("pandas")
excel_plugin = pytest.importorskip("dbt.adapters.duckdb.plugins.excel")


def test_skip_empty_sheet_does_not_open_workbook(tmp_path, monkeypatch):
    output = tmp_path / "existing.xlsx"
    plugin = BasePlugin.create(
        "excel", config={"output": {"file": str(output), "skip_empty_sheet": True}}
    )
    target = TargetConfig(
        relation=Mock(identifier="report"),
        column_list=[],
        config={},
        location=TargetLocation("reports/region=west/part.parquet", "parquet"),
    )
    dataframe = pandas.DataFrame({"value": [None], "region": ["west"]})
    monkeypatch.setattr(excel_plugin.pd_utils, "target_to_df", lambda _: dataframe)
    writer = Mock()
    monkeypatch.setattr(excel_plugin.pd, "ExcelWriter", writer)

    plugin.store(target)

    writer.assert_not_called()
