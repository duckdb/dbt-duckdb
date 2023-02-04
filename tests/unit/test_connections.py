from unittest import mock

from botocore.credentials import Credentials

from dbt.adapters.duckdb.connections import DuckDBCredentials


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
