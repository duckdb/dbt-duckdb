import unittest
from unittest import mock

from botocore.exceptions import UnauthorizedSSOTokenError
from botocore.credentials import Credentials

from dbt.adapters.duckdb.credentials.s3_credential_provider import get_s3_credentials
from dbt.adapters.duckdb import DuckDBCredentials


class TestCredentials(unittest.TestCase):

    def test_get_s3_credentials_static_credentials(self):
        credentials = DuckDBCredentials()
        credentials.settings = {'s3_access_key_id': 'key_id', 's3_secret_access_key': 'secret_key',
                                's3_session_token': 'session_token', 's3_region': 'region'}

        s3_credentials = get_s3_credentials(credentials).to_dict()

        self.assertEqual(s3_credentials['s3_access_key_id'], 'key_id')
        self.assertEqual(s3_credentials['s3_secret_access_key'], 'secret_key')
        self.assertEqual(s3_credentials['s3_session_token'], 'session_token')
        self.assertEqual(s3_credentials['s3_region'], 'region')

    def test_get_s3_credentials_invalid_credential_provider(self):
        credentials = DuckDBCredentials()
        credentials.use_credential_provider = 'unexistingProvider'

        with self.assertRaises(RuntimeError) as context:
            get_s3_credentials(credentials)

        self.assertTrue('Unsupported credential provider specified' in str(context.exception))

    def test_get_s3_credentials_no_credential_provider(self):
        credentials = DuckDBCredentials()
        credentials.use_credential_provider = ''

        s3_credentials = get_s3_credentials(credentials).to_dict()

        self.assertEqual(s3_credentials, {})

    @mock.patch("boto3.session.Session")
    def test_get_s3_credentials_valid_aws_credential_provider(self, mock_session_class):
        mock_session_object = mock.Mock()
        mock_client = mock.Mock()

        mock_session_object.get_credentials.return_value = Credentials('access_key', 'secret_key', 'token')
        mock_session_object.client.return_value = mock_client
        mock_session_class.return_value = mock_session_object
        mock_client.get_caller_identity.return_value = {}
        credentials = DuckDBCredentials()
        credentials.use_credential_provider = 'aws'

        s3_credentials = get_s3_credentials(credentials).to_dict()

        self.assertEqual(s3_credentials['s3_access_key_id'], 'access_key')
        self.assertEqual(s3_credentials['s3_secret_access_key'], 'secret_key')
        self.assertEqual(s3_credentials['s3_session_token'], 'token')

    @mock.patch("boto3.session.Session")
    def test_get_s3_credentials_aws_credential_provider_with_exception(self, mock_session_class):
        mock_session_object = mock.Mock()
        mock_client = mock.Mock()

        mock_session_object.client.return_value = mock_client
        mock_session_class.return_value = mock_session_object
        mock_session_object.client.return_value = mock_client
        mock_client.get_caller_identity.side_effect = UnauthorizedSSOTokenError()
        credentials = DuckDBCredentials()
        credentials.use_credential_provider = 'aws'

        with self.assertRaises(UnauthorizedSSOTokenError):
            get_s3_credentials(credentials).to_dict()
