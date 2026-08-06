from pathlib import Path
from typing import Any

import pytest

SUPPORTED_DUCKLAKE_PROFILES = {"memory", "md"}


def ducklake_metadata_schema(database_name: str) -> str:
    """Catalog holding the DuckLake metadata tables (``ducklake_table``, ...).

    Local DuckLake attaches this catalog itself; managed DuckLake does not
    expose it, so tests attach it under the same name.
    """
    return f"__ducklake_metadata_{database_name}"


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


@pytest.mark.requires_ducklake
@pytest.mark.skip_profile("buenavista")
class BaseDucklakeIntegration:
    """Attaches a DuckLake database so tests can inspect its metadata catalog."""

    @pytest.fixture(scope="class", autouse=True)
    def skip_non_ducklake_motherduck(self, profile_type, database_type):
        if profile_type == "md" and database_type != "ducklake":
            pytest.skip("requires the managed DuckLake MotherDuck profile")

    @pytest.fixture(scope="class")
    def ducklake_attachment(self, test_database_name, profile_type, tmp_path_factory, request):
        if profile_type == "md":
            metadata_db = ducklake_metadata_schema(test_database_name)
            return {
                "path": f"md:{metadata_db}",
                "alias": metadata_db,
                "type": "motherduck",
            }

        root = Path(tmp_path_factory.mktemp(request.cls.__name__.lower()))
        metadata_path = root / "metadata.ducklake"
        data_path = root / "data"
        data_path.mkdir(parents=True, exist_ok=True)

        return {
            "path": f"ducklake:sqlite:{metadata_path}",
            "alias": test_database_name,
            "options": {"data_path": str(data_path)},
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, ducklake_attachment, profile_type):
        target = dict(dbt_profile_target)
        if profile_type == "md":
            target["is_ducklake"] = True
        else:
            target["path"] = target.get("path", ":memory:")
        target["attach"] = [ducklake_attachment]
        return {
            "test": {
                "outputs": {"dev": target},
                "target": "dev",
            }
        }
