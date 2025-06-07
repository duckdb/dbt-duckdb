import pytest
from dbt.adapters.duckdb.credentials import Attachment


class TestDataPathQuoting:
    """Test that data_path options are properly quoted in SQL generation."""

    def test_data_path_s3_url_should_be_quoted(self):
        """Test that S3 URLs in data_path are properly quoted."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": "s3://my-bucket/path"}
        )
        sql = attachment.to_sql()
        # Should generate: ATTACH '/tmp/test.db' (DATA_PATH 's3://my-bucket/path')
        assert "DATA_PATH 's3://my-bucket/path'" in sql

    def test_data_path_windows_path_should_be_quoted(self):
        """Test that Windows paths in data_path are properly quoted."""
        attachment = Attachment(
            path="/tmp/test.db", 
            options={"data_path": "C:\\Users\\test\\data"}
        )
        sql = attachment.to_sql()
        # Should generate: ATTACH '/tmp/test.db' (DATA_PATH 'C:\Users\test\data')
        assert "DATA_PATH 'C:\\Users\\test\\data'" in sql

    def test_data_path_unix_path_should_be_quoted(self):
        """Test that Unix paths in data_path are properly quoted."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": "/home/user/data"}
        )
        sql = attachment.to_sql()
        # Should generate: ATTACH '/tmp/test.db' (DATA_PATH '/home/user/data')
        assert "DATA_PATH '/home/user/data'" in sql

    def test_data_path_url_with_spaces_should_be_quoted(self):
        """Test that paths with spaces are properly quoted."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": "/path/with spaces/data"}
        )
        sql = attachment.to_sql()
        # Should generate: ATTACH '/tmp/test.db' (DATA_PATH '/path/with spaces/data')
        assert "DATA_PATH '/path/with spaces/data'" in sql

    def test_numeric_options_should_not_be_quoted(self):
        """Test that numeric options are not quoted."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"timeout": 30000}
        )
        sql = attachment.to_sql()
        # Should generate: ATTACH '/tmp/test.db' (TIMEOUT 30000)
        assert "TIMEOUT 30000" in sql
        assert "TIMEOUT '30000'" not in sql

    def test_boolean_options_work_correctly(self):
        """Test that boolean options work as expected."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"use_cache": True, "skip_validation": False}
        )
        sql = attachment.to_sql()
        # True booleans should appear as flag, False booleans should be omitted
        assert "USE_CACHE" in sql
        assert "SKIP_VALIDATION" not in sql

    def test_multiple_options_with_data_path(self):
        """Test multiple options including data_path."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={
                "data_path": "s3://bucket/path",
                "timeout": 5000,
                "use_cache": True
            }
        )
        sql = attachment.to_sql()
        assert "DATA_PATH 's3://bucket/path'" in sql
        assert "TIMEOUT 5000" in sql
        assert "USE_CACHE" in sql

    def test_already_single_quoted_strings_not_double_quoted(self):
        """Test that already single-quoted strings are not double-quoted."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": "'s3://my-bucket/path'"}
        )
        sql = attachment.to_sql()
        # Should keep existing single quotes, not add more
        assert "DATA_PATH 's3://my-bucket/path'" in sql
        assert "DATA_PATH ''s3://my-bucket/path''" not in sql

    def test_already_double_quoted_strings_preserved(self):
        """Test that already double-quoted strings are preserved."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": '"s3://my-bucket/path"'}
        )
        sql = attachment.to_sql()
        # Should keep existing double quotes
        assert 'DATA_PATH "s3://my-bucket/path"' in sql
        assert 'DATA_PATH \'"s3://my-bucket/path"\'' not in sql

    def test_quoted_strings_with_whitespace_preserved(self):
        """Test that quoted strings with surrounding whitespace are preserved."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": "  's3://my-bucket/path'  "}
        )
        sql = attachment.to_sql()
        # Should detect quotes despite whitespace and preserve original value
        assert "DATA_PATH   's3://my-bucket/path'  " in sql
        assert "DATA_PATH '  's3://my-bucket/path'  '" not in sql

    def test_quoted_strings_with_whitespace_double_quotes(self):
        """Test that double quoted strings with surrounding whitespace are preserved."""
        attachment = Attachment(
            path="/tmp/test.db",
            options={"data_path": '  "s3://my-bucket/path"  '}
        )
        sql = attachment.to_sql()
        # Should detect quotes despite whitespace and preserve original value
        assert 'DATA_PATH   "s3://my-bucket/path"  ' in sql
        assert 'DATA_PATH \'  "s3://my-bucket/path"  \'' not in sql