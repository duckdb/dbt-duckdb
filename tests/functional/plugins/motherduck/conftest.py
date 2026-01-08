import uuid
from datetime import datetime
import pytest
import duckdb
import os


@pytest.fixture(scope="session")
def test_database_name():
    """Generate a unique database name for the entire motherduck test session"""
    date_str = datetime.now().strftime("%Y%m%d")
    random_suffix = uuid.uuid4().hex[:6]
    db_name = f"test_db_{date_str}_{random_suffix}"

    # Create the database once for all tests
    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("TEST_MOTHERDUCK_TOKEN")
    if token:
        conn = duckdb.connect(f"md:?motherduck_token={token}")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.close()

    yield db_name

    # Clean up: drop the database after all tests complete
    if token:
        conn = duckdb.connect(f"md:?motherduck_token={token}")
        conn.execute(f"DROP DATABASE IF EXISTS {db_name}")
        conn.close()
