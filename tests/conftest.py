import os
import random
import resource
import string
import subprocess
import time
from importlib import metadata

import duckdb
import pytest

from tests.ducklake import configure_ducklake_profile

# Increase the number of open files allowed
# Hack for https://github.com/dbt-labs/dbt-core/issues/7316
soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

MOTHERDUCK_TOKEN = "MOTHERDUCK_TOKEN"
TEST_MOTHERDUCK_TOKEN = "TEST_MOTHERDUCK_TOKEN"
PROFILE_TYPES = ("memory", "file", "md", "buenavista", "nightly")
DATABASE_TYPES = ("duckdb", "ducklake")
CONFIG_SKIP_MARKERS = {
    "skip_profile": ("--profile", PROFILE_TYPES, "profile"),
    "skip_database_type": ("--database-type", DATABASE_TYPES, "database type"),
}

# This option cleans up each test's duckdb instance as soon as the duckdbPyConnection
# closes instead of allowing it to live in the instance cache for reuse on the next
# `duckdb.connect(<same path>)`.
# Every test profile whose *primary* database is `md:{database_name}` must include
# this in its config_options: a lingering instance from a profile without it makes
# the next test's connection to the same `md:{database_name}`` path fail with
# "Can't open a connection to same database file with a different configuration
# than existing connections".
MD_TEST_CONFIG_OPTIONS = {"motherduck_dbinstance_inactivity_ttl": "0s"}


def create_motherduck_database_sql(database_name: str, database_type: str) -> str:
    if database_type == "ducklake":
        return f"CREATE OR REPLACE DATABASE {database_name} (TYPE ducklake)"
    return f"CREATE OR REPLACE DATABASE {database_name}"


def pytest_addoption(parser):
    parser.addoption(
        "--profile", action="store", choices=PROFILE_TYPES, default="memory", type=str
    )
    parser.addoption(
        "--database-type",
        action="store",
        choices=DATABASE_TYPES,
        default="duckdb",
        type=str,
    )


def pytest_report_header() -> list[str]:
    """Return a list of strings to be displayed in the header of the report."""
    return [
        f"duckdb: {metadata.version('duckdb')}",
        f"dbt-core: {metadata.version('dbt-core')}",
    ]


@pytest.fixture(scope="session")
def profile_type(request):
    return request.config.getoption("--profile")


@pytest.fixture(scope="session")
def database_type(request):
    return request.config.getoption("--database-type")


@pytest.fixture(scope="session")
def test_database_name(database_type):
    """Generate a unique database name for the entire MotherDuck test session

    The suffix is deliberately letters-only (no digits): several functional
    tests normalize compiled SQL by stripping numeric noise (e.g. `re.sub(r"\\d+", "")`)
    before comparing it against an expected string built from the raw database
    name, so a database name containing digits would get mangled on one side
    of the comparison but not the other.
    """
    random_suffix = "".join(random.choices(string.ascii_lowercase, k=12))
    db_name = f"test_db_{random_suffix}"

    # Create the database once for all tests
    token = os.environ.get(MOTHERDUCK_TOKEN) or os.environ.get(TEST_MOTHERDUCK_TOKEN)
    if token:
        conn = duckdb.connect(f"md:?motherduck_token={token}")
        conn.execute(create_motherduck_database_sql(db_name, database_type))
        conn.close()

    yield db_name

    # Clean up: drop the database after all tests complete
    if token:
        conn = duckdb.connect(f"md:?motherduck_token={token}")
        conn.execute(f"DROP DATABASE IF EXISTS {db_name}")
        conn.close()


@pytest.fixture(scope="session")
def bv_server_process(profile_type):
    if profile_type == "buenavista":
        server_process = subprocess.Popen(["python3", "-m", "tests.bv_test_server"])

        # Wait for the server to be ready
        time.sleep(5)

        # Pass the server process to the tests
        yield server_process

        # Teardown: Stop the server process after tests are done
        server_process.terminate()
        server_process.wait()
    else:
        yield None


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="session")
def dbt_profile_target(profile_type, database_type, bv_server_process, tmpdir_factory, request):
    profile = {"type": "duckdb", "threads": 4}

    if profile_type == "buenavista":
        profile["database"] = "memory"
        profile["remote"] = {
            "host": "127.0.0.1",
            "port": 5433,
            "user": "test",
        }
    elif profile_type == "file":
        profile["path"] = str(tmpdir_factory.mktemp("dbs") / "tmp.db")
    elif profile_type == "md":
        # Test against MotherDuck
        if MOTHERDUCK_TOKEN not in os.environ and MOTHERDUCK_TOKEN.lower() not in os.environ:
            if TEST_MOTHERDUCK_TOKEN not in os.environ:
                raise ValueError(
                    f"Please set the {MOTHERDUCK_TOKEN} or {TEST_MOTHERDUCK_TOKEN} \
                        environment variable to run tests against MotherDuck"
                )
            profile["token"] = os.environ.get(TEST_MOTHERDUCK_TOKEN)
        else:
            profile["token"] = os.environ.get(MOTHERDUCK_TOKEN, os.environ.get(MOTHERDUCK_TOKEN.lower()))
        profile["disable_transactions"] = True
        db_name = request.getfixturevalue("test_database_name")
        profile["path"] = f"md:{db_name}"
        profile["config_options"] = dict(MD_TEST_CONFIG_OPTIONS)
    elif profile_type in ["memory", "nightly"]:
        pass  # use the default path-less profile
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    if database_type == "ducklake":
        configure_ducklake_profile(profile, profile_type, tmpdir_factory)

    return profile


@pytest.fixture(scope="session")
def test_data_path():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(test_dir, "data")


def mark_skipped_by_config(items, marker_name, selected_value):
    """Apply skip marks based on the testing configuration of a particular run.

    pytest_collection_modifyitems() calls this function for each entry
    on CONFIG_SKIP_MARKERS.
    """
    try:
        _, known_values, value_description = CONFIG_SKIP_MARKERS[marker_name]
    except KeyError:
        raise pytest.UsageError(f"Unknown config skip marker: {marker_name}") from None

    for item in items:
        for marker in item.iter_markers(marker_name):
            unknown_values = [value for value in marker.args if value not in known_values]
            if not marker.args or unknown_values:
                supplied_values = ", ".join(repr(value) for value in marker.args) or "none"
                expected_values = ", ".join(repr(value) for value in known_values)
                raise pytest.UsageError(
                    f"{item.nodeid}: {marker_name} has unknown {value_description} "
                    f"value(s) {supplied_values}; expected one or more of {expected_values}"
                )

            if selected_value in marker.args:
                reason = marker.kwargs.get(
                    "reason", f"skipped on '{selected_value}' {value_description}"
                )
                item.add_marker(pytest.mark.skip(reason=reason))
                break


def pytest_collection_modifyitems(config, items):
    """Apply skip marker during collection.

    Implementing skips this way offers two benefits:
    - It allows to verify that the skip parameters are valid (no typos).
    - Skips are applied before any fixture is initialized, so only fixtures
    that are needed for each testing config are initialized.

    Pytest calls this hook after collecting test items, but before initializing anything.
    - It converts the custom profile and database-type markers into Pytest skips,
    - Also skips tests when there are no valid s3 credentials or the DuckLake extension is unavailable.
    """
    for marker_name, (option_name, _, _) in CONFIG_SKIP_MARKERS.items():
        mark_skipped_by_config(items, marker_name, config.getoption(option_name))

    skip_s3 = None
    # Skip the S3 tests if the secrets are not available
    if not (
        os.getenv("S3_MD_ORG_KEY") and os.getenv("S3_MD_ORG_REGION") and os.getenv("S3_MD_ORG_SECRET")
    ):
        skip_s3 = pytest.mark.skip(reason="need S3 credentials to run this test")

    # Skip s3 tests if httpfs extension is unavailable
    try:
        duckdb.sql("install httpfs")
    except duckdb.Error as e:
        if "Failed to download extension \"httpfs\"" in str(e):
            skip_s3 = pytest.mark.skip(reason="httpfs not available and is needed for setting s3 credentials")

    # Skip ducklake tests if the extension is unavailable
    skip_ducklake = None
    try:
        duckdb.sql("install ducklake")
    except duckdb.Error as e:
        if "Failed to download extension" in str(e):
            skip_ducklake = pytest.mark.skip(reason="ducklake extension not available")

    if skip_s3 is not None:
        for item in items:
            if "with_s3_creds" in item.keywords:
                item.add_marker(skip_s3)

    if skip_ducklake is not None:
        for item in items:
            if "requires_ducklake" in item.keywords:
                item.add_marker(skip_ducklake)
