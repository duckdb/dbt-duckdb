import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


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
