from unittest import mock

import duckdb
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.duckdb import extension_repositories as er
from dbt.adapters.duckdb.credentials import DuckDBCredentials, Extension, ExtensionRepository
from dbt.adapters.duckdb.environments import Environment

# Throwaway 2048 bit RSA public keys: DuckDB validates the key format when a
# repository is created, but never contacts the repository unless we install from it
KEY_1 = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzaZUt/QipxzO9vp8d9ia
BOqeSFXqsaVHlJkHeGAWIRbftpEHRfYbj3cfIOIp/Muf1IvNzJIBjftWwscVOly3
qv7QcCq0tjEjDPbG0k3hvqpFiJEwcEeGPlMY4l2+A7mMyIusRd5vAlJVh2eKZ5KG
arnmdFu4HFgsy6lU85M67WYEbiRQKNSRZZdLehDWtio9aU7A1D2uVBhhgPJ0QfMl
AD8p+/rtfwKClDOkV+n5V3q4L1+v90fjd0xmQuRjox3x/IOxGEmD5Fek+iG6w4Ef
ockP7F9SU0XaivMsIQWnMdiKyPl6QX1943ZMPKLqnToy2R0dYPcjVFMz7BuOtx6G
jQIDAQAB
-----END PUBLIC KEY-----
"""

KEY_2 = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtbb77g6eYAB0L2jODeFw
gCECKiIRAATRTjG0ZNUIzA0b6a0QVwHJRQnaEQvVXMzTxJI3BhPUATT2tz8tc28b
sOxEzvW09DEGCb4+Gkd63ZVo5Rz6KJ0x9M1rI/09GKbJzJ3Q6zi3EWbdKhfhFCzr
W0wEN6CCHZ/9EJMqM2ianCSokAeGE23cGgqdbracQ1ntREIdyXT6xbibvH3ryMGu
6fb8hcSy0PYQDiZyebPnOIHKhWNIM3LuCY09YnClyy/iWrVfzIi1XCAx12QC/rWj
MTm2/YLlDqwIBpgamqDdeuy8OBSRUkDKMECcub69KShatvEg1XYe2XU3jU83maVl
eQIDAQAB
-----END PUBLIC KEY-----
"""

PREFIX = "https://extensions.example.invalid"

requires_duckdb_2 = pytest.mark.skipif(
    not er.supports_extension_repositories(),
    reason="Custom extension repositories require DuckDB 2.0+",
)


def test_parse_extension_repositories():
    creds = DuckDBCredentials.from_dict(
        dict(
            extension_repositories=[
                dict(name="query_farm", prefix=PREFIX),
                dict(name="pinned-repo", prefix=PREFIX, public_key=KEY_1),
            ]
        )
    )
    assert creds.extension_repositories == [
        ExtensionRepository(name="query_farm", prefix=PREFIX),
        ExtensionRepository(name="pinned-repo", prefix=PREFIX, public_key=KEY_1),
    ]


@pytest.mark.parametrize("name", ["", "my repo", 'bad"name', "a;b"])
def test_invalid_repository_name(name):
    with pytest.raises(DbtRuntimeError, match="Invalid extension repository name"):
        ExtensionRepository(name=name, prefix=PREFIX)


def test_builtin_repository_name():
    with pytest.raises(DbtRuntimeError, match="built into DuckDB"):
        ExtensionRepository(name="community", prefix=PREFIX)


def test_missing_prefix():
    with pytest.raises(DbtRuntimeError, match="must specify a prefix"):
        ExtensionRepository(name="repo", prefix="")


def test_create_repository_sql_escapes_values():
    repo = ExtensionRepository(name="repo", prefix="https://x.invalid/it's", public_key="ab'c")
    assert er.create_repository_sql(repo) == (
        "CREATE EXTENSION REPOSITORY IF NOT EXISTS \"repo\" "
        "WITH PREFIX 'https://x.invalid/it''s' USING PUBLIC KEY 'ab''c'"
    )
    assert er.create_repository_sql(ExtensionRepository(name="repo", prefix=PREFIX)) == (
        f"CREATE EXTENSION REPOSITORY IF NOT EXISTS \"repo\" WITH PREFIX '{PREFIX}'"
    )


def test_normalize_public_key():
    body = "".join(line for line in KEY_1.splitlines() if "-----" not in line)
    assert er.normalize_public_key(KEY_1) == body
    assert er.normalize_public_key(f"  {body}\n") == body


def test_requires_duckdb_2():
    conn = mock.Mock()
    with mock.patch.object(er.duckdb, "__version__", "1.5.5"):
        with pytest.raises(DbtRuntimeError, match="requires DuckDB 2.0.0 or later"):
            er.register_extension_repositories(
                conn, [ExtensionRepository(name="repo", prefix=PREFIX)]
            )
    conn.execute.assert_not_called()


def test_no_repositories_is_a_noop():
    conn = mock.Mock()
    er.register_extension_repositories(conn, [])
    conn.execute.assert_not_called()


@pytest.mark.parametrize("repo", ["core", "community", "core_nightly"])
def test_builtin_repository_extension_keeps_bare_load(repo):
    conn = mock.Mock()
    er.install_and_load_extension(conn, Extension(name="h3", repo=repo))
    conn.execute.assert_called_once_with(f"install h3 from {repo}")
    conn.load_extension.assert_called_once_with("h3")


def test_custom_repository_extension_loads_from_repository():
    conn = mock.Mock()
    with mock.patch.object(er.duckdb, "__version__", "2.0.0"):
        er.install_and_load_extension(conn, Extension(name="a5", repo="query-farm"))
    assert conn.execute.call_args_list == [
        mock.call('INSTALL a5 FROM "query-farm"'),
        mock.call('LOAD a5 FROM "query-farm"'),
    ]
    conn.load_extension.assert_not_called()


def test_custom_repository_extension_before_duckdb_2():
    conn = mock.Mock()
    with mock.patch.object(er.duckdb, "__version__", "1.5.5"):
        er.install_and_load_extension(conn, Extension(name="a5", repo="my_repo"))
    conn.execute.assert_called_once_with("install a5 from my_repo")
    conn.load_extension.assert_called_once_with("a5")


def _connect(repo_dir, **config):
    return duckdb.connect(config={"extension_repository_directory": str(repo_dir), **config})


def _repositories(conn):
    return {
        name: prefix
        for name, prefix, kind in conn.execute(
            "SELECT repository_name, prefix, type FROM duckdb_extension_repositories()"
        ).fetchall()
        if kind == "USER_PROVIDED"
    }


@requires_duckdb_2
def test_register_and_reuse(tmp_path):
    repos = [ExtensionRepository(name="repo", prefix=PREFIX, public_key=KEY_1)]
    conn = _connect(tmp_path)
    er.register_extension_repositories(conn, repos)
    assert _repositories(conn) == {"repo": PREFIX}
    conn.close()

    # Repositories persist; reusing a matching one must not require opting in again
    conn = _connect(tmp_path)
    er.register_extension_repositories(conn, repos)
    assert conn.execute("SELECT current_setting('allow_extension_repositories')").fetchone()[
        0
    ] == "undecided"

    # The base64 body of the key and a trailing slash on the prefix still match
    body = "".join(line for line in KEY_1.splitlines() if "-----" not in line)
    er.register_extension_repositories(
        conn, [ExtensionRepository(name="repo", prefix=PREFIX + "/", public_key=body)]
    )
    # Not pinning a key accepts whatever key was pinned before
    er.register_extension_repositories(conn, [ExtensionRepository(name="repo", prefix=PREFIX)])


@requires_duckdb_2
def test_prefix_mismatch(tmp_path):
    conn = _connect(tmp_path)
    er.register_extension_repositories(
        conn, [ExtensionRepository(name="repo", prefix=PREFIX, public_key=KEY_1)]
    )
    with pytest.raises(DbtRuntimeError, match="already registered with prefix"):
        er.register_extension_repositories(
            conn,
            [ExtensionRepository(name="repo", prefix="https://other.invalid", public_key=KEY_1)],
        )


@requires_duckdb_2
def test_public_key_mismatch(tmp_path):
    conn = _connect(tmp_path)
    er.register_extension_repositories(
        conn, [ExtensionRepository(name="repo", prefix=PREFIX, public_key=KEY_1)]
    )
    with pytest.raises(DbtRuntimeError, match="public key that does not match"):
        er.register_extension_repositories(
            conn, [ExtensionRepository(name="repo", prefix=PREFIX, public_key=KEY_2)]
        )


@requires_duckdb_2
def test_forbidden(tmp_path):
    conn = _connect(tmp_path, allow_extension_repositories="forbidden")
    with pytest.raises(DbtRuntimeError, match="'forbidden'"):
        er.register_extension_repositories(
            conn, [ExtensionRepository(name="repo", prefix=PREFIX, public_key=KEY_1)]
        )
    assert _repositories(conn) == {}


@requires_duckdb_2
def test_initialize_db_registers_repositories(tmp_path):
    creds = DuckDBCredentials.from_dict(
        dict(
            config_options={"extension_repository_directory": str(tmp_path)},
            extension_repositories=[dict(name="repo", prefix=PREFIX, public_key=KEY_1)],
        )
    )
    conn = Environment.initialize_db(creds)
    assert _repositories(conn) == {"repo": PREFIX}
