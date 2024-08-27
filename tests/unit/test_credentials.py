import duckdb
import pytest
from unittest import mock

from botocore.credentials import Credentials

from dbt.adapters.duckdb.credentials import Attachment, DuckDBCredentials


def test_load_basic_settings():
    settings = {
        "access_mode": "read_only",
        "log_query_path": "/path/to/log",
    }
    creds = DuckDBCredentials(settings=settings)
    assert creds.settings == settings


def test_add_secret_with_empty_name():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="s3",
                name="",
                key_id="abc",
                secret="xyz",
                region="us-west-2"
            )
        ]
    )
    assert len(creds.secrets) == 1
    assert creds._secrets[0].type == "s3"
    assert creds._secrets[0].secret_kwargs.get("key_id") == "abc"
    assert creds._secrets[0].secret_kwargs.get("secret") == "xyz"
    assert creds._secrets[0].secret_kwargs.get("region") == "us-west-2"

    sql = creds.secrets_sql()[0]
    assert sql == \
"""CREATE SECRET (
    type s3,
    key_id 'abc',
    secret 'xyz',
    region 'us-west-2'
)"""


def test_add_secret_with_name():
    creds = DuckDBCredentials.from_dict(
        dict(secrets=[
            dict(
                type="s3",
                name="my_secret",
                key_id="abc",
                secret="xyz",
                region="us-west-2",
                scope="s3://my-bucket"
            )
        ])
    )
    assert len(creds._secrets) == 1
    assert creds._secrets[0].type == "s3"
    assert creds._secrets[0].secret_kwargs.get("key_id") == "abc"
    assert creds._secrets[0].secret_kwargs.get("secret") == "xyz"
    assert creds._secrets[0].secret_kwargs.get("region") == "us-west-2"
    assert creds._secrets[0].scope == "s3://my-bucket"

    sql = creds.secrets_sql()[0]
    assert sql == \
"""CREATE OR REPLACE SECRET my_secret (
    type s3,
    scope 's3://my-bucket',
    key_id 'abc',
    secret 'xyz',
    region 'us-west-2'
)"""


def test_add_unsupported_secret():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="scrooge_mcduck",
                name="money"
            )
        ]
    )
    sql = creds.secrets_sql()[0]
    assert sql == \
"""CREATE OR REPLACE SECRET money (
    type scrooge_mcduck
)"""
    with pytest.raises(duckdb.InvalidInputException) as e:
        duckdb.sql(sql)
        assert "Secret type 'scrooge_mcduck' not found" in str(e)


def test_add_unsupported_secret_param():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="s3",
                password="secret"
            )
        ]
    )
    sql = creds.secrets_sql()[0]
    assert sql == \
"""CREATE OR REPLACE SECRET __default_s3 (
    type s3,
    password 'secret'
)"""
    with pytest.raises(duckdb.BinderException) as e:
        duckdb.sql(sql)
        msg = "Unknown parameter 'password' for secret type 's3' with default provider 'config'"
        assert msg in str(e)


def test_add_azure_secret():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="azure",
                name="",
                provider="service_principal",
                tenant_id="abc",
                client_id="xyz",
                client_certificate_path="foo\\bar\\baz.pem",
                account_name="123"
            )
        ]
    )
    assert len(creds.secrets) == 1
    assert creds._secrets[0].type == "azure"
    assert creds._secrets[0].secret_kwargs.get("tenant_id") == "abc"
    assert creds._secrets[0].secret_kwargs.get("client_id") == "xyz"
    assert creds._secrets[0].secret_kwargs.get("client_certificate_path") == "foo\\bar\\baz.pem"
    assert creds._secrets[0].secret_kwargs.get("account_name") == "123"

    sql = creds.secrets_sql()[0]
    assert sql == \
"""CREATE SECRET (
    type azure,
    provider service_principal,
    tenant_id 'abc',
    client_id 'xyz',
    client_certificate_path 'foo\\bar\\baz.pem',
    account_name '123'
)"""


def test_add_hf_secret():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="huggingface",
                name="",
                token="abc"
            )
        ]
    )
    assert len(creds.secrets) == 1
    assert creds._secrets[0].type == "huggingface"
    assert creds._secrets[0].secret_kwargs.get("token") == "abc"

    sql = creds.secrets_sql()[0]
    assert sql == \
"""CREATE SECRET (
    type huggingface,
    token 'abc'
)"""


@mock.patch("boto3.session.Session")
def test_load_aws_creds(mock_session_class):
    mock_session_object = mock.Mock()
    mock_client = mock.Mock()

    mock_session_object.get_credentials.return_value = Credentials(
        "access_key", "secret_key", "token"
    )
    mock_session_object.client.return_value = mock_client
    mock_session_class.return_value = mock_session_object
    mock_client.get_caller_identity.return_value = {}

    creds = DuckDBCredentials(use_credential_provider="aws")
    assert len(creds.secrets) == 1
    assert creds._secrets[0].type == "s3"
    assert creds._secrets[0].provider == "credential_chain"


def test_attachments():
    creds = DuckDBCredentials()
    creds.attach = [
        {"path": "/tmp/f1234.db"},
        {"path": "/tmp/g1234.db", "alias": "g"},
        {"path": "/tmp/h5678.db", "read_only": 1},
        {"path": "/tmp/i9101.db", "type": "sqlite"},
        {"path": "/tmp/jklm.db", "alias": "jk", "read_only": 1, "type": "sqlite"},
    ]

    expected_sql = [
        "ATTACH '/tmp/f1234.db'",
        "ATTACH '/tmp/g1234.db' AS g",
        "ATTACH '/tmp/h5678.db' (READ_ONLY)",
        "ATTACH '/tmp/i9101.db' (TYPE sqlite)",
        "ATTACH '/tmp/jklm.db' AS jk (TYPE sqlite, READ_ONLY)",
    ]

    for i, a in enumerate(creds.attach):
        attachment = Attachment(**a)
        assert expected_sql[i] == attachment.to_sql()


def test_infer_database_name_from_path():
    payload = {}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "memory"

    payload = {"path": "local.duckdb"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "local"

    payload = {"path": "/tmp/f1234.db"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "f1234"

    payload = {"path": "md:?token=abc123"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "my_db"

    payload = {"path": "md:jaffle_shop?token=abc123"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "jaffle_shop"

    payload = {"database": "memory"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "memory"

    payload = {
        "database": "remote",
        "remote": {"host": "localhost", "port": 5433, "user": "test"},
    }
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "remote"
