"""End-to-end tests for submitting Python models to MotherDuck Flights.

These run real Flights, which cost MotherDuck compute and take ~10s each, and
require an account with Flights enabled. They are opt-in: set
DBT_DUCKDB_TEST_FLIGHTS=1 to run them (`tox -e md-flights`).
"""

import os

import pytest
from dbt.tests.util import run_dbt

FLIGHTS_ENABLED = os.getenv("DBT_DUCKDB_TEST_FLIGHTS", "") in ("1", "true", "True")


class FlightTestBase:
    @pytest.fixture(autouse=True)
    def require_flights(self, project):
        if not FLIGHTS_ENABLED:
            pytest.skip(
                "Set DBT_DUCKDB_TEST_FLIGHTS=1 to run tests that execute real MotherDuck Flights"
            )
        # Turning the flag on for an account without Flights should skip, not
        # fail: this suite would otherwise take down the whole MotherDuck job.
        try:
            project.run_sql('select 1 from MD_LIST_FLIGHTS("limit" := 1)', fetch="all")
        except Exception as err:
            pytest.skip(f"MotherDuck Flights are not available on this account: {err}")

upstream_sql = """
{{ config(materialized='table') }}
select i as id, i % 3 as grp from generate_series(1, 100) g(i)
"""

flight_python_model = """
import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="flight",
        packages=["pandas==2.2.3"],
    )
    df = dbt.ref("upstream_model").df()
    return df.groupby("grp", as_index=False)["id"].count().rename(columns={"id": "n"})
"""

local_python_model = """
def model(dbt, session):
    dbt.config(materialized="table", submission_method="local")
    return dbt.ref("upstream_model")
"""


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMotherDuckFlightPythonModel(FlightTestBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.sql": upstream_sql,
            "flight_model.py": flight_python_model,
        }

    def test_python_model_runs_on_a_flight(self, project):
        run_dbt(["run"])

        # The Flight wrote the table from its own session; this assertion also
        # covers the cross-session visibility the incremental path depends on.
        rows = project.run_sql("select grp, n from flight_model order by grp", fetch="all")
        assert [(row[0], row[1]) for row in rows] == [(0, 33), (1, 34), (2, 33)]

    def test_rerun_reuses_the_flight(self, project):
        # Unchanged code must not mint a new Flight version on every dbt run,
        # so assert on the version itself: the row count is identical either
        # way and would not catch a regression in the reuse check.
        run_dbt(["run"])

        # Resolved in two steps: MotherDuck's table functions reject subqueries
        # in their arguments.
        (flight_id,) = project.run_sql(
            'select flight_id from MD_LIST_FLIGHTS("limit" := 200, owner_only := true) '
            "where flight_name like '%-flight_model'",
            fetch="one",
        )
        version_sql = f"select current_version from MD_GET_FLIGHT(flight_id := '{flight_id}')"
        (before,) = project.run_sql(version_sql, fetch="one")

        run_dbt(["run"])
        (after,) = project.run_sql(version_sql, fetch="one")

        assert after == before
        (count,) = project.run_sql("select count(*) from flight_model", fetch="one")
        assert count == 3


@pytest.mark.skip_profile("buenavista", "file", "memory")
class TestMotherDuckFlightsEnabledByDefault(FlightTestBase):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, test_database_name):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": f"md:{test_database_name}",
                        # config_options carries MD_TEST_CONFIG_OPTIONS, which every
                        # profile whose primary database is md:{database_name} needs:
                        # without it a lingering instance makes the next test's
                        # connection to the same path fail (see tests/conftest.py).
                        "config_options": dbt_profile_target.get("config_options"),
                        "is_ducklake": dbt_profile_target.get("is_ducklake"),
                        "flights": {"enabled_by_default": True},
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.sql": upstream_sql,
            # No submission_method: it should pick up the profile default
            "default_model.py": flight_python_model.replace(
                '        submission_method="flight",\n', ""
            ),
            # ...and an individual model can still opt back out
            "opted_out_model.py": local_python_model,
        }

    def test_profile_default_and_per_model_override(self, project):
        run_dbt(["run"])
        (default_rows,) = project.run_sql("select count(*) from default_model", fetch="one")
        (opted_out_rows,) = project.run_sql("select count(*) from opted_out_model", fetch="one")
        assert default_rows == 3
        assert opted_out_rows == 100
