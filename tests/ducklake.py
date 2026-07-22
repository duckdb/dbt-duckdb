from pathlib import Path
from typing import Any


DUCKLAKE_DATABASE_TYPE = "ducklake"
SUPPORTED_DUCKLAKE_PROFILES = {"memory", "md"}


def configure_ducklake_profile(
    profile: dict[str, Any], profile_type: str, tmpdir_factory: Any
) -> None:
    if profile_type not in SUPPORTED_DUCKLAKE_PROFILES:
        supported = ", ".join(sorted(SUPPORTED_DUCKLAKE_PROFILES))
        raise ValueError(
            f"DuckLake databases are only supported for these test profiles: {supported}"
        )

    # Concurrent dbt writers can lose DuckLake staging relations across connections.
    profile["threads"] = 1

    if profile_type == "memory":
        root = Path(tmpdir_factory.mktemp("ducklake"))
        profile["path"] = f"ducklake:{root / 'catalog.ducklake'}"
    else:
        profile["is_ducklake"] = True


def create_motherduck_database_sql(database_name: str, database_type: str) -> str:
    if database_type == DUCKLAKE_DATABASE_TYPE:
        return f"CREATE DATABASE {database_name} (TYPE ducklake)"
    return f"CREATE DATABASE IF NOT EXISTS {database_name}"
