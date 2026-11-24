import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.util import run_dbt


@pytest.mark.skip_profile("buenavista")
class TestUnitTestingTypesDuckDB(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["2.0", "2.0"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'2013-11-03 00:00:00-0'::TIMESTAMPTZ", "2013-11-03 00:00:00-0"],
            [
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
            ],
            ["ARRAY['a','b','c']", "['a','b','c']"],
            ["ARRAY[1,2,3]", "[1, 2, 3]"],
        ]


class TestUnitTestCaseInsensitivityDuckDB(BaseUnitTestCaseInsensivity):
    pass


class TestUnitTestInvalidInputDuckDB(BaseUnitTestInvalidInput):
    pass


@pytest.mark.skip_profile("buenavista")
class TestUnitTestTimeZoneDuckDB:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        return {
            **dbt_profile_target,
            "settings": {
                **dbt_profile_target.get("settings", {}),
                "TimeZone": "Australia/Melbourne",
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "events.sql": "select '2025-06-01 23:00:00'::timestamptz as occurred_at",
            "event_dates.sql": """
                select occurred_at::date as event_date
                from {{ ref('events') }}
            """,
            "unit_tests.yml": """
unit_tests:
  - name: interprets_fixture_timestamp_in_profile_timezone
    model: event_dates
    given:
      - input: ref('events')
        rows:
          - {occurred_at: "2025-06-01 23:00:00"}
          - {occurred_at: "2025-06-01 23:00:00+00:00"}
    expect:
      rows:
        - {event_date: "2025-06-01"}
        - {event_date: "2025-06-02"}
""",
        }

    def test_fixture_timestamps_follow_profile_timezone(self, project):
        run_dbt(["run", "--select", "events"])
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
