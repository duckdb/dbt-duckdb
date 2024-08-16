from pbr.version import VersionInfo

package_name='dbt-duckdb'
info = VersionInfo(package_name)

version = info.version_string()
