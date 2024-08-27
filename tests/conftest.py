import os
import resource
import subprocess
import time

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
def dbt_profile_target(profile_type, bv_server_process, tmp_path_factory):
    profile = {"type": "duckdb", "threads": 4}

    if profile_type == "buenavista":
        profile["database"] = "memory"
        profile["remote"] = {
            "host": "127.0.0.1",
            "port": 5433,
            "user": "test",
        }
    elif profile_type == "file":
        profile["path"] = str(tmp_path_factory.getbasetemp() / "tmp.db")
    elif profile_type == "md":
        # Test against MotherDuck
        if MOTHERDUCK_TOKEN not in os.environ and MOTHERDUCK_TOKEN.lower() not in os.environ:
            if TEST_MOTHERDUCK_TOKEN not in os.environ:
                raise ValueError(
                    f"Please set the {MOTHERDUCK_TOKEN} or {TEST_MOTHERDUCK_TOKEN} \
                        environment variable to run tests against MotherDuck"
                )
            profile["token"] = os.environ.get(TEST_MOTHERDUCK_TOKEN)
        profile["disable_transactions"] = True
        profile["path"] = "md:test"
    elif profile_type == "unity":
        profile["extensions"] = [{"name": "uc_catalog",
                                  "repository": "http://nightly-extensions.duckdb.org"}]
        profile["attach"] = [
            {"path": "unity",
             "alias": "unity",
             "type": "UC_CATALOG"},
        ]
        profile["secrets"] = [{
            "type": "UC",
            # here our mock uc server is running, prism defaults to 4010
            "endpoint": "http://127.0.0.1:4010",
            "token": "test",
            "aws_region": "eu-west-1"
        }]
    elif profile_type == "memory":
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
