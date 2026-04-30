import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.duckdb.impl import _render_partition_part


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("col1", '"col1"'),
        ("time field", '"time field"'),
        ('with"quote', '"with""quote"'),
        ("day(ts)", "day(ts)"),
        ('day("ts")', 'day("ts")'),
        ('day("time field")', 'day("time field")'),
        ("bucket(16, user_id)", "bucket(16, user_id)"),
        ('bucket(16, "user_id")', 'bucket(16, "user_id")'),
        ("  day(ts)  ", "day(ts)"),
        ("Day(TS)", "Day(TS)"),
        ("quarter(ts)", "quarter(ts)"),
    ],
)
def test_render_partition_part_ok(raw, expected):
    assert _render_partition_part(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "day(ts",
        "day(ts))",
        "day(ts); drop table x; --",
        "day(ts) -- comment",
        "day(ts; drop table x;)",
        "day(ts) drop table x",
        "1day(ts)",
        "day-fn(ts)",
        "(ts)",
    ],
)
def test_render_partition_part_rejected(raw):
    with pytest.raises(DbtRuntimeError):
        _render_partition_part(raw)
