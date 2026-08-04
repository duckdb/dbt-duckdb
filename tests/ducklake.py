from pathlib import Path
from typing import Any

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
