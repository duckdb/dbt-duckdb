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

    if profile_type == "memory":
        path = ":memory:"
    elif profile_type == "database":
        path = str(tmp_path_factory.getbasetemp() / "tmp.db")
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    return {
        "type": "duckdb",
        "threads": 4,
        "path": path,
    }
