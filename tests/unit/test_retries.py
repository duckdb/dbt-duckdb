import pytest
from unittest.mock import MagicMock
from unittest.mock import patch

import duckdb

from dbt.adapters.duckdb.credentials import Retries
from dbt.adapters.duckdb.environments import RetryableCursor

class TestRetryableCursor:

    @pytest.fixture
    def mock_cursor(self):
        return MagicMock()

    @pytest.fixture
    def mock_retries(self):
        return Retries(query_attempts=3)

    @pytest.fixture
    def retry_cursor(self, mock_cursor, mock_retries):
        return RetryableCursor(
            mock_cursor,
            mock_retries.query_attempts,
            mock_retries.retryable_exceptions)

    def test_successful_execute(self, mock_cursor, retry_cursor):
        """ Test that execute successfully runs the SQL query. """
        sql_query = "SELECT * FROM table"
        retry_cursor.execute(sql_query)
        mock_cursor.execute.assert_called_once_with(sql_query)

    def test_retry_on_failure(self, mock_cursor, retry_cursor):
        """ Test that execute retries the SQL query on failure. """
        mock_cursor.execute.side_effect = [duckdb.duckdb.IOException, None]
        sql_query = "SELECT * FROM table"
        retry_cursor.execute(sql_query)
        assert mock_cursor.execute.call_count == 2

    def test_no_retry_on_non_retryable_exception(self, mock_cursor, retry_cursor):
        """ Test that a non-retryable exception is not retried. """
        mock_cursor.execute.side_effect = ValueError
        sql_query = "SELECT * FROM table"
        with pytest.raises(ValueError):
            retry_cursor.execute(sql_query)
        mock_cursor.execute.assert_called_once_with(sql_query)

    def test_exponential_backoff(self, mock_cursor, retry_cursor):
        """ Test that exponential backoff is applied between retries. """
        mock_cursor.execute.side_effect = [duckdb.duckdb.IOException, duckdb.duckdb.IOException, None]
        sql_query = "SELECT * FROM table"

        with patch("time.sleep") as mock_sleep:
            retry_cursor.execute(sql_query)
            assert mock_sleep.call_count == 2
            mock_sleep.assert_any_call(1)
            mock_sleep.assert_any_call(2)
