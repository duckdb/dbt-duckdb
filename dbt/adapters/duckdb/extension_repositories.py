from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

import duckdb
from dbt_common.exceptions import DbtRuntimeError
from packaging.version import Version

from .credentials import BUILTIN_EXTENSION_REPOSITORIES
from .credentials import Extension
from .credentials import EXTENSION_REPOSITORY_NAME_PATTERN
from .credentials import ExtensionRepository
from .utils import escape_sql_string

# Custom, signed extension repositories were introduced in DuckDB 2.0
MIN_EXTENSION_REPOSITORY_VERSION = Version("2.0.0")


def supports_extension_repositories(version: Optional[str] = None) -> bool:
    return Version(version or duckdb.__version__) >= MIN_EXTENSION_REPOSITORY_VERSION


def normalize_public_key(key: str) -> str:
    """Reduce a PEM (or bare base64) public key to its base64 body, which is how
    DuckDB reports keys in duckdb_extension_repositories()."""
    body = [line for line in key.strip().splitlines() if not line.startswith("-----")]
    return "".join("".join(body).split())


def _normalize_prefix(prefix: str) -> str:
    return prefix.rstrip("/")


def create_repository_sql(repo: ExtensionRepository) -> str:
    sql = (
        f'CREATE EXTENSION REPOSITORY IF NOT EXISTS "{repo.name}" '
        f"WITH PREFIX '{escape_sql_string(repo.prefix)}'"
    )
    if repo.public_key:
        sql += f" USING PUBLIC KEY '{escape_sql_string(repo.public_key.strip())}'"
    return sql


def _existing_repositories(conn) -> Dict[str, Tuple[str, List[str]]]:
    rows = conn.execute(
        "SELECT repository_name, prefix, public_keys FROM duckdb_extension_repositories()"
    ).fetchall()
    return {name: (prefix, list(keys or [])) for name, prefix, keys in rows}


def _check_repository(repo: ExtensionRepository, prefix: str, keys: List[str]):
    if _normalize_prefix(prefix) != _normalize_prefix(repo.prefix):
        raise DbtRuntimeError(
            f"Extension repository '{repo.name}' is already registered with prefix "
            f"'{prefix}', which does not match the configured prefix '{repo.prefix}'. "
            f'Run DROP EXTENSION REPOSITORY "{repo.name}" (or choose another name) '
            "if the repository's trust configuration should change."
        )
    if repo.public_key and normalize_public_key(repo.public_key) not in keys:
        raise DbtRuntimeError(
            f"Extension repository '{repo.name}' is already registered with a public key "
            "that does not match the configured public_key. "
            f'Run DROP EXTENSION REPOSITORY "{repo.name}" (or choose another name) '
            "if the repository's trust configuration should change."
        )


def register_extension_repositories(conn, repos: Sequence[ExtensionRepository]):
    """Ensure that each declared repository is registered with a matching prefix and key,
    creating the ones that do not exist yet."""
    if not repos:
        return
    if not supports_extension_repositories():
        raise DbtRuntimeError(
            "The extension_repositories profile setting requires DuckDB "
            f"{MIN_EXTENSION_REPOSITORY_VERSION} or later (found {duckdb.__version__})"
        )

    # 'forbidden' also distrusts repositories that already exist, so their
    # extensions could not be loaded even if we did not need to create them
    allowed = conn.execute("SELECT current_setting('allow_extension_repositories')").fetchone()[0]
    if str(allowed).lower() == "forbidden":
        raise DbtRuntimeError(
            "The profile declares extension_repositories, but allow_extension_repositories "
            "is set to 'forbidden' on this connection"
        )

    existing = _existing_repositories(conn)
    missing = []
    for repo in repos:
        if repo.name in existing:
            _check_repository(repo, *existing[repo.name])
        else:
            missing.append(repo)

    if not missing:
        return

    # Declaring a repository in the profile is an explicit opt-in to adding it
    if str(allowed).lower() != "allowed":
        conn.execute("SET allow_extension_repositories = 'allowed'")
    for repo in missing:
        conn.execute(create_repository_sql(repo))

    # IF NOT EXISTS guards against another process registering the same repository
    # concurrently; re-check so that a mismatched concurrent registration is reported
    existing = _existing_repositories(conn)
    for repo in missing:
        _check_repository(repo, *existing[repo.name])


def install_and_load_extension(conn, ext: Extension):
    """Install and load an extension that is configured as a name/repo pair."""
    custom_repo = (
        ext.repo not in BUILTIN_EXTENSION_REPOSITORIES
        and EXTENSION_REPOSITORY_NAME_PATTERN.match(ext.repo)
        and supports_extension_repositories()
    )
    if custom_repo:
        # DuckDB stores extensions from custom repositories separately, so a bare
        # LOAD would not find them: the repository is needed for both steps
        conn.execute(f'INSTALL {ext.name} FROM "{ext.repo}"')
        conn.execute(f'LOAD {ext.name} FROM "{ext.repo}"')
    else:
        conn.execute(f"install {ext.name} from {ext.repo}")
        conn.load_extension(ext.name)
