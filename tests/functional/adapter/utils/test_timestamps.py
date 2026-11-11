import pytest

from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps
from dbt.tests.util import run_dbt

_MODEL_UTC_DRIFT = """
select
    epoch({{ current_timestamp_in_utc_backcompat() }}) - epoch(now()) as utc_drift_seconds
"""


class TestCurrentTimestamps(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "TIMESTAMP WITH TIME ZONE",
            "current_timestamp_in_utc_backcompat": "TIMESTAMP",
            "current_timestamp_backcompat": "TIMESTAMP",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
        select now() as current_timestamp,
               (now() at time zone 'utc')::timestamp as current_timestamp_in_utc_backcompat,
               current_timestamp::timestamp as current_timestamp_backcompat
        """


# Buena Vista runs the queries on its own server session, so the profile's
# TimeZone setting never reaches it.
@pytest.mark.skip_profile("buenavista")
class TestCurrentTimestampInUtcWithLocalTimeZone:
    """current_timestamp_in_utc_backcompat() must return UTC wall-clock time even
    when the session TimeZone is not UTC (DuckDB defaults it to the local zone)."""

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        return {
            **dbt_profile_target,
            "settings": {
                **dbt_profile_target.get("settings", {}),
                "TimeZone": "Asia/Kathmandu",
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"utc_drift.sql": _MODEL_UTC_DRIFT}

    def test_current_timestamp_in_utc_backcompat(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        timezone = project.run_sql("select current_setting('TimeZone')", fetch="one")[0]
        assert timezone == "Asia/Kathmandu"

        drift = project.run_sql(
            f"select utc_drift_seconds from {project.test_schema}.utc_drift", fetch="one"
        )[0]
        assert abs(drift) < 60
