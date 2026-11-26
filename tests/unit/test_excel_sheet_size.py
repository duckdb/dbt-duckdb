from unittest.mock import Mock

import pytest

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig

pandas = pytest.importorskip("pandas")
excel_plugin = pytest.importorskip("dbt.adapters.duckdb.plugins.excel")


@pytest.mark.parametrize("ignore_size", [None, False, True])
def test_oversized_sheet_respects_ignore_setting(tmp_path, monkeypatch, ignore_size):
    output = {"file": str(tmp_path / "report.xlsx")}
    if ignore_size is not None:
        output["ignore_sheet_too_large"] = ignore_size
    plugin = BasePlugin.create("excel", config={"output": output})
    target = TargetConfig(relation=Mock(identifier="report"), column_list=[], config={})
    dataframe = pandas.DataFrame({"value": [1, 2]})
    monkeypatch.setattr(excel_plugin.pd_utils, "target_to_df", lambda _: dataframe)
    monkeypatch.setattr(excel_plugin.pd, "ExcelWriter", Mock())
    written = []

    def write_sheet(frame, writer, **kwargs):
        if len(frame) > 1:
            raise ValueError("This sheet is too large!")
        written.append(frame)

    monkeypatch.setattr(pandas.DataFrame, "to_excel", write_sheet)

    if ignore_size:
        plugin.store(target)
        assert len(written) == 1
        assert written[0]["Error"].tolist() == ["This sheet is too large!"]
    else:
        with pytest.raises(ValueError, match="This sheet is too large!"):
            plugin.store(target)
        assert not written
