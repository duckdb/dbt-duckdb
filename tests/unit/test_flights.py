import duckdb
import pytest

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.duckdb.credentials import DuckDBCredentials, FlightConfig
from dbt.adapters.duckdb.environments.flights import (
    FlightRunner,
    build_requirements,
    build_source,
    flight_name,
)
from dbt.adapters.duckdb.environments.motherduck import MotherDuckEnvironment

COMPILED_CODE = """
def model(dbt, session):
    return dbt.ref("upstream")
"""


def parsed_model(**config):
    return {
        "package_name": "my_project",
        "database": "my_db",
        "schema": "main",
        "alias": "my_model",
        "config": config,
    }


def credentials(**kwargs):
    # from_dict so `database` is derived from `path` the way dbt does it
    return DuckDBCredentials.from_dict({"schema": "main", "path": "md:my_db", **kwargs})


class FakeCursor:
    """Stands in for a DuckDB cursor, recording SQL and replaying canned rows."""

    def __init__(self, responses):
        self.responses = responses
        self.executed = []
        self._rows = []

    def execute(self, sql, bindings=None):
        self.executed.append(sql)
        for prefix, rows in self.responses.items():
            if prefix in sql:
                self._rows = rows() if callable(rows) else rows
                return self
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def sql_containing(self, needle):
        return [sql for sql in self.executed if needle in sql]


def test_flight_name_is_deterministic_and_qualified():
    name = flight_name(parsed_model())
    assert name == "dbt-my_project-my_db-main-my_model"
    assert name == flight_name(parsed_model())


def test_flight_name_sanitizes_and_respects_prefix():
    model = parsed_model()
    model["schema"] = "dbt.dev schema"
    assert flight_name(model, prefix="ci") == "ci-my_project-my_db-dbt_dev_schema-my_model"


def test_flight_name_truncates_with_digest():
    model = parsed_model()
    model["alias"] = "x" * 300
    name = flight_name(model)
    assert len(name) <= 120
    # The identifier tail survives, and the digest keeps truncation collision-free
    assert name.endswith(flight_name(model)[-9:])


def test_build_source_appends_entrypoint():
    source = build_source(COMPILED_CODE)
    assert source.startswith("def model(dbt, session):")
    assert "def main():" in source
    assert 'if __name__ == "__main__":' in source


def test_build_source_rejects_oversized_model():
    with pytest.raises(DbtRuntimeError, match="too large to run as a MotherDuck Flight"):
        build_source("# " + "x" * (200 * 1024))


def test_build_requirements_pins_local_duckdb_version():
    requirements = build_requirements(parsed_model(), FlightConfig())
    assert requirements.splitlines()[0] == f"duckdb=={duckdb.__version__}"


def test_build_requirements_includes_model_packages():
    requirements = build_requirements(
        parsed_model(packages=["scikit-learn==1.5.0"]), FlightConfig()
    )
    assert "scikit-learn==1.5.0" in requirements.splitlines()


def test_build_requirements_lets_the_model_pin_duckdb():
    requirements = build_requirements(parsed_model(packages=["duckdb==1.4.0"]), FlightConfig())
    assert requirements.splitlines() == ["duckdb==1.4.0"]


def test_build_requirements_adds_profile_requirements():
    config = FlightConfig(requirements=["pandas==2.2.3"], duckdb_version="1.5.5")
    requirements = build_requirements(parsed_model(), config).splitlines()
    assert requirements == ["duckdb==1.5.5", "pandas==2.2.3"]


def test_model_packages_override_profile_requirements():
    # Two pins for one distribution would make the installer fail before the
    # model ever runs, so the more specific one has to win outright.
    config = FlightConfig(requirements=["pandas==2.2.3"], duckdb_version="1.5.5")
    requirements = build_requirements(parsed_model(packages=["pandas==2.1.0"]), config)
    assert requirements.splitlines() == ["duckdb==1.5.5", "pandas==2.1.0"]


def test_build_requirements_normalizes_distribution_names():
    config = FlightConfig(requirements=["scikit_learn==1.4.0"])
    requirements = build_requirements(parsed_model(packages=["Scikit-Learn==1.5.0"]), config)
    assert "scikit_learn==1.4.0" not in requirements
    assert "Scikit-Learn==1.5.0" in requirements


def test_build_requirements_handles_extras_and_markers():
    config = FlightConfig(requirements=["pandas[performance]==2.2.3"])
    requirements = build_requirements(parsed_model(packages=["pandas==2.1.0"]), config)
    assert "pandas[performance]==2.2.3" not in requirements
    assert "pandas==2.1.0" in requirements


def test_build_requirements_passes_pip_options_through():
    config = FlightConfig(requirements=["--index-url https://example.com/simple", "pandas==2.2.3"])
    lines = build_requirements(parsed_model(), config).splitlines()
    assert lines[0] == "--index-url https://example.com/simple"
    assert "pandas==2.2.3" in lines


def test_build_source_applies_profile_settings():
    source = build_source(COMPILED_CODE, {"TimeZone": "UTC"})
    assert "SET TimeZone = 'UTC'" in source
    # ...on the write cursor too, matching Environment.initialize_cursor
    assert source.count("__dbt_apply_settings(") == 3


def test_build_source_without_settings_is_a_no_op():
    assert "__dbt_settings = []" in build_source(COMPILED_CODE)


def _runner_cursor(status="SUCCEEDED", existing=None):
    """A cursor wired for the create-run-poll path."""
    return FakeCursor(
        {
            "MD_LIST_FLIGHTS": existing or [],
            "MD_CREATE_FLIGHT": [("11111111-2222-3333-4444-555555555555",)],
            "MD_UPDATE_FLIGHT": [("11111111-2222-3333-4444-555555555555",)],
            # MD_GET_FLIGHT_VERSION has to be matched before MD_GET_FLIGHT_RUN
            # and MD_GET_FLIGHT, which are prefixes of nothing but share a stem
            "MD_GET_FLIGHT_VERSION": [("stale source", "stale requirements")],
            "MD_GET_FLIGHT(": [(2,)],
            "MD_RUN_FLIGHT": [(7,)],
            "MD_GET_FLIGHT_RUN": [(status, 0 if status == "SUCCEEDED" else 1)],
            "MD_GET_FLIGHT_LOGS": [("Traceback (most recent call last):",), ("ValueError: nope",)],
        }
    )


def test_submit_creates_runs_and_waits():
    cursor = _runner_cursor()
    response = FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)

    assert response._message == "OK"
    assert cursor.sql_containing("MD_CREATE_FLIGHT")
    assert cursor.sql_containing("MD_RUN_FLIGHT")
    assert cursor.sql_containing("MD_GET_FLIGHT_RUN")
    # The generated entrypoint is what gets shipped
    assert "def main():" in cursor.sql_containing("MD_CREATE_FLIGHT")[0]


def test_submit_reuses_an_unchanged_flight():
    existing = [("11111111-2222-3333-4444-555555555555", "dbt-my_project-my_db-main-my_model")]
    cursor = _runner_cursor(existing=existing)
    source = build_source(COMPILED_CODE)
    requirements = build_requirements(parsed_model(), FlightConfig())
    cursor.responses["MD_GET_FLIGHT_VERSION"] = [(source, requirements)]

    FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)

    # An unchanged model must not push a new Flight version on every dbt run
    assert not cursor.sql_containing("MD_UPDATE_FLIGHT")
    assert not cursor.sql_containing("MD_CREATE_FLIGHT")
    assert cursor.sql_containing("MD_RUN_FLIGHT")


def test_submit_updates_a_changed_flight():
    existing = [("11111111-2222-3333-4444-555555555555", "dbt-my_project-my_db-main-my_model")]
    cursor = _runner_cursor(existing=existing)

    FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)

    assert cursor.sql_containing("MD_UPDATE_FLIGHT")
    assert not cursor.sql_containing("MD_CREATE_FLIGHT")


def test_submit_raises_with_logs_when_the_run_fails():
    cursor = _runner_cursor(status="FAILED")
    with pytest.raises(DbtRuntimeError) as excinfo:
        FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)

    message = str(excinfo.value)
    assert "status FAILED" in message
    assert "ValueError: nope" in message


def test_submit_cancels_the_run_when_it_times_out():
    cursor = _runner_cursor(status="RUNNING")
    cursor.responses["MD_CANCEL_FLIGHT_RUN"] = [(True,)]

    with pytest.raises(DbtRuntimeError) as excinfo:
        # timeout_sec=0 means the deadline has already passed on the first poll
        FlightRunner(FlightConfig(timeout_sec=0)).submit(cursor, parsed_model(), COMPILED_CODE)

    # An abandoned run would commit the table after dbt gave up on the node
    assert cursor.sql_containing("MD_CANCEL_FLIGHT_RUN")
    assert "was cancelled" in str(excinfo.value)


def test_submit_reports_a_failed_cancel_without_masking_the_timeout():
    cursor = _runner_cursor(status="RUNNING")  # no MD_CANCEL_FLIGHT_RUN response -> raises

    with pytest.raises(DbtRuntimeError) as excinfo:
        FlightRunner(FlightConfig(timeout_sec=0)).submit(cursor, parsed_model(), COMPILED_CODE)

    message = str(excinfo.value)
    assert "Timed out" in message
    assert "could not be cancelled" in message


def test_submit_passes_optional_flight_settings():
    cursor = _runner_cursor()
    config = FlightConfig(access_token_name="analytics-token", max_runtime_sec=900)
    FlightRunner(config).submit(cursor, parsed_model(), COMPILED_CODE)

    create = cursor.sql_containing("MD_CREATE_FLIGHT")[0]
    assert "access_token_name := 'analytics-token'" in create
    assert "max_runtime_sec := 900" in create


def _env(creds=None, **flight_kwargs):
    creds = creds or credentials()
    if flight_kwargs:
        creds.flights = FlightConfig(**flight_kwargs)
    return MotherDuckEnvironment(creds)


def test_submission_method_defaults_to_local():
    assert _env().submission_method(parsed_model()) == "local"


def test_submission_method_follows_the_model_config():
    assert _env().submission_method(parsed_model(submission_method="flight")) == "flight"


def test_submission_method_profile_default_can_be_overridden_per_model():
    env = _env(enabled_by_default=True)
    assert env.submission_method(parsed_model()) == "flight"
    assert env.submission_method(parsed_model(submission_method="local")) == "local"


def test_submission_method_rejects_unknown_values():
    with pytest.raises(DbtRuntimeError, match="Unsupported submission_method"):
        _env().submission_method(parsed_model(submission_method="spark"))


def test_flight_target_accepts_a_motherduck_database():
    _env().validate_flight_target(parsed_model())


def test_flight_target_rejects_a_local_primary_connection():
    # MotherDuck attached to a local database still selects MotherDuckEnvironment,
    # but a Flight cannot see the local `memory` catalog the model targets.
    creds = DuckDBCredentials.from_dict(
        {
            "database": "memory",
            "schema": "main",
            "path": ":memory:",
            "attach": [{"path": "md:my_db"}],
        }
    )
    model = parsed_model()
    model["database"] = "memory"
    with pytest.raises(DbtRuntimeError) as excinfo:
        _env(creds).validate_flight_target(model)
    # and the error names the database that *is* reachable
    assert "not reachable from a MotherDuck Flight" in str(excinfo.value)
    assert "my_db" in str(excinfo.value)


def test_flight_target_rejects_a_profile_with_no_reachable_database():
    # Every MotherDuck attachment is aliased, so none of them resolve remotely
    creds = DuckDBCredentials.from_dict(
        {
            "database": "memory",
            "schema": "main",
            "path": ":memory:",
            "attach": [{"path": "md:my_db", "alias": "aliased"}],
        }
    )
    model = parsed_model()
    model["database"] = "aliased"
    with pytest.raises(DbtRuntimeError, match="only be submitted to MotherDuck Flights"):
        _env(creds).validate_flight_target(model)


def test_flight_target_accepts_an_unaliased_motherduck_attachment():
    creds = DuckDBCredentials.from_dict(
        {
            "database": "memory",
            "schema": "main",
            "path": ":memory:",
            "attach": [{"path": "md:my_db"}],
        }
    )
    _env(creds).validate_flight_target(parsed_model())


def test_flight_target_rejects_a_database_the_flight_cannot_see():
    # An aliased attachment resolves locally but not inside the Flight, whose
    # fresh md: connection only knows MotherDuck's own database names.
    creds = DuckDBCredentials.from_dict(
        {
            "database": "my_db",
            "schema": "main",
            "path": "md:my_db",
            "attach": [{"path": "md:other_db", "alias": "aliased"}],
        }
    )
    model = parsed_model()
    model["database"] = "aliased"
    with pytest.raises(DbtRuntimeError, match="not reachable from a MotherDuck Flight"):
        _env(creds).validate_flight_target(model)


def test_flight_runner_is_shared_and_carries_profile_settings():
    env = _env(credentials(settings={"TimeZone": "UTC"}))
    runner = env.flight_runner()
    assert env.flight_runner() is runner
    assert runner._settings == {"TimeZone": "UTC"}


def test_flights_block_parses_from_a_profile():
    creds = DuckDBCredentials.from_dict(
        {
            "database": "my_db",
            "schema": "main",
            "path": "md:my_db",
            "flights": {
                "enabled_by_default": True,
                "access_token_name": "analytics-token",
                "max_runtime_sec": 900,
                "requirements": ["pandas==2.2.3"],
            },
        }
    )
    assert isinstance(creds.flights, FlightConfig)
    assert creds.flights.enabled_by_default is True
    assert creds.flights.access_token_name == "analytics-token"
    assert creds.flights.max_runtime_sec == 900
    assert creds.flights.requirements == ["pandas==2.2.3"]
