from importlib.metadata import version as get_version

_package_name = "dbt-duckdb"
version = get_version(_package_name)
# This is to get around SemVer 2 (dbt_common) vs Linux/Python compatible SemVer 3 (pbr) conflicting
# See: https://docs.openstack.org/pbr/latest/user/semver.html
_prerelease_tags = ["dev", "a", "b", "c"]
for tag in _prerelease_tags:
    version = version.replace(f".{tag}", f"-{tag}")
