from unittest import mock

from botocore.credentials import Credentials

from dbt.adapters.duckdb.credentials import Attachment, DuckDBCredentials


def test_load_basic_settings():
    creds = DuckDBCredentials()
    creds.settings = {
        "s3_access_key_id": "abc",
        "s3_secret_access_key": "xyz",
        "s3_region": "us-west-2",
    }
    settings = creds.load_settings()
    assert creds.settings == settings


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
    creds.settings = {"some_other_setting": 1}

    settings = creds.load_settings()
    assert settings["s3_access_key_id"] == "access_key"
    assert settings["s3_secret_access_key"] == "secret_key"
    assert settings["s3_session_token"] == "token"
    assert settings["some_other_setting"] == 1


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
