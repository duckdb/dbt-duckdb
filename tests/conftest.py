import os
import resource
import subprocess
import time
from importlib import metadata

import duckdb
import pytest

# Increase the number of open files allowed
# Hack for https://github.com/dbt-labs/dbt-core/issues/7316
soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

MOTHERDUCK_TOKEN = "MOTHERDUCK_TOKEN"
TEST_MOTHERDUCK_TOKEN = "TEST_MOTHERDUCK_TOKEN"


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="memory", type=str)


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
def dbt_profile_target(profile_type, bv_server_process, tmpdir_factory):
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
        profile["path"] = "md:test"
    elif profile_type in ["memory", "nightly"]:
        pass  # use the default path-less profile
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    return profile


@pytest.fixture(autouse=True, scope="class")
def skip_by_profile_type(profile_type, request):
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")


@pytest.fixture(scope="session")
def test_data_path():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(test_dir, "data")


def pytest_collection_modifyitems(config, items):
    # Skip the S3 tests if the secrets are not available
    if not (
        os.getenv("S3_MD_ORG_KEY") and os.getenv("S3_MD_ORG_REGION") and os.getenv("S3_MD_ORG_SECRET")
    ):
        skip_s3 = pytest.mark.skip(reason="need S3 credentials to run this test")
        for item in items:
            if "with_s3_creds" in item.keywords:
                item.add_marker(skip_s3)
