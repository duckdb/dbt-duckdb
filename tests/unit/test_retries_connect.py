import pytest
from unittest.mock import patch

from duckdb.duckdb import IOException

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.credentials import Retries
from dbt.adapters.duckdb.environments import Environment

class TestConnectRetries:

    @pytest.fixture
    def creds(self):
        # Create a mock credentials object
        return DuckDBCredentials(
            path="foo.db",
            retries=Retries(connect_attempts=2, retryable_exceptions=["IOException", "ArithmeticError"])
        )

    @pytest.mark.parametrize("exception", [None, IOException, ArithmeticError, ValueError])
    def test_initialize_db(self, creds, exception):
        # Mocking the duckdb.connect method
        with patch('duckdb.connect') as mock_connect:
            if exception:
                mock_connect.side_effect = [exception, None]

            if exception == ValueError:
                with pytest.raises(ValueError) as excinfo:
                    Environment.initialize_db(creds)
            else:
                # Call the initialize_db method
                Environment.initialize_db(creds)
                if exception in {IOException, ArithmeticError}:
                    assert mock_connect.call_count == creds.retries.connect_attempts
                else:
                    mock_connect.assert_called_once_with(creds.path, read_only=False, config={})
