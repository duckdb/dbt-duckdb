import subprocess
import time

import pytest


# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]


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
@pytest.fixture(scope="class")
def dbt_profile_target(profile_type, bv_server_process, tmp_path_factory):
    profile = {"type": "duckdb", "threads": 4}

    if profile_type == "buenavista":
        profile["database"] = "memory"
        profile["remote"] = {
            "host": "127.0.0.1",
            "port": 5433,
            "user": "test",
        }
    elif profile_type == "memory":
        profile["path"] = ":memory:"
    elif profile_type == "file":
        profile["path"] = str(tmp_path_factory.getbasetemp() / "tmp.db")
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    return profile


@pytest.fixture(autouse=True)
def skip_by_profile_type(profile_type, request):
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")
