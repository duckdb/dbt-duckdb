from importlib.metadata import version as get_version

__package_name__ = "dbt-duckdb"
version = get_version(__package_name__)
# This is to get around SemVer 2 (dbt_common) vs Linux/Python compatible SemVer 3 (pbr) conflicting
# See: https://docs.openstack.org/pbr/latest/user/semver.html
version = version.replace(".dev", "-dev")
