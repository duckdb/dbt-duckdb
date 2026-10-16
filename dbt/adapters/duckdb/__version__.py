from importlib.metadata import version as get_version

_package_name = "dbt-duckdb"
version = get_version(_package_name)
# dbt_common.semver rejects PEP 440 pre-release forms like "1.10.1.dev3", so
# rewrite them to the SemVer-style "1.10.1-dev3". hatch-vcs is configured with
# local_scheme = "no-local-version" so no "+g<sha>" local segment is emitted.
_prerelease_tags = ["dev", "a", "b", "c"]
for tag in _prerelease_tags:
    version = version.replace(f".{tag}", f"-{tag}")
