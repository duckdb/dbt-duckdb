import pytest

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="memory", type=str)


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target(request, tmp_path_factory):
    profile_type = request.config.getoption("--profile")

    profile = {
        "type": "duckdb",
        "threads": 4
    }

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
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")
