import os
import pytest
from testcontainers.postgres import PostgresContainer
import duckdb
from dbt.adapters.duckdb.plugins.postgres import Plugin
from dbt.adapters.events.logging import AdapterLogger
from dbt.tests.util import run_dbt

logger = AdapterLogger("DuckDB_PostgresPlugin")

# Skipping this b/c it requires running a properly setup Postgres server
# when testing it locally and also b/c I think there is something
# wrong with profiles_config_update since it can't be used in multiple
# tests in the same pytest session
# 
# Exercise locally with: pytest --profile=file tests/functional/plugins/test_postgres.py
# there are networking issues testing this in a devcontainer, more here https://github.com/testcontainers/testcontainers-python/issues/538
# the test works outside the devcontainer though
@pytest.mark.skip
@pytest.mark.usefixtures("postgres_container")
class TestPostgresPlugin:
    @pytest.fixture(scope="class")
    def postgres_container(request):
        """
        Fixture to set up a PostgreSQL container using Testcontainers.
        """
        postgres = PostgresContainer("postgres:16-alpine")
        postgres.start()
        os.environ["DB_HOST"] = postgres.get_container_host_ip()
        os.environ["DB_PORT"] = postgres.get_exposed_port(5432)
        os.environ["DB_USERNAME"] = postgres.username
        os.environ["DB_PASSWORD"] = postgres.password
        os.environ["DB_NAME"] = postgres.dbname

        # Log the environment variables for debugging
        logger.info("Postgres container environment variables:")
        logger.info(f"DB_HOST={os.environ['DB_HOST']}")
        logger.info(f"DB_PORT={os.environ['DB_PORT']}")
        logger.info(f"DB_USERNAME={os.environ['DB_USERNAME']}")
        logger.info(f"DB_PASSWORD={os.environ['DB_PASSWORD']}")
        logger.info(f"DB_NAME={os.environ['DB_NAME']}")

        yield postgres

        # Cleanup after tests
        postgres.stop()


    @pytest.fixture(scope="class")
    def dsn(self):
        """
        Fixture to construct the DSN from environment variables.
        """
        dsn = (
            f"host={os.environ['DB_HOST']} "
            f"port={os.environ['DB_PORT']} "
            f"dbname={os.environ['DB_NAME']} "
            f"user={os.environ['DB_USERNAME']} "
            f"password={os.environ['DB_PASSWORD']}"
        )
        yield dsn

    @pytest.fixture(scope="class")
    def plugin_config(self, dsn):
        """
        Default plugin configuration.
        """
        config = {
            "dsn": dsn,
            "duckdb_alias": "postgres_db",
            "pg_schema": "public",
            "read_only": False,
            "settings": {
                "pg_use_binary_copy": True,
                "pg_debug_show_queries": False,
            },
            "attach_options": {
                # Additional ATTACH options can be added here
            }
        }
        yield config

    @pytest.fixture(scope="class")
    def plugin_instance(self, plugin_config):
        """
        Fixture to initialize the PostgreSQL plugin with the provided configuration.
        """
        plugin = Plugin(name="postgres", plugin_config=plugin_config)
        yield plugin

    @pytest.fixture(scope="class")
    def duckdb_connection(self, plugin_instance):
        """
        Fixture to create and configure an in-memory DuckDB connection.
        """
        conn = duckdb.connect(database=':memory:', read_only=False)
        plugin_instance.configure_connection(conn)
        yield conn
        conn.close()

    @pytest.fixture(scope="function", autouse=True)
    def setup_data(self, duckdb_connection):
        """
        Fixture to set up data before each test.
        """
        # Ensure the table does not already exist
        duckdb_connection.execute("DROP TABLE IF EXISTS postgres_db.public.foo")

        # Create table in PostgreSQL via DuckDB connection
        duckdb_connection.execute("""
            CREATE TABLE postgres_db.public.foo (
                id INTEGER PRIMARY KEY,
                i INTEGER
            )
        """)
        duckdb_connection.execute("INSERT INTO postgres_db.public.foo (id, i) VALUES (1, 2), (2, 2), (3, 2)")

    @pytest.fixture
    def duckdb_connection_factory(self, dsn):
        """
        Fixture to create a function that returns a DuckDB connection configured with the plugin.
        """
        def _factory(plugin_config):
            plugin = Plugin(name="postgres", plugin_config=plugin_config)
            conn = duckdb.connect(database=':memory:', read_only=False)
            plugin.configure_connection(conn)
            return conn
        yield _factory

    def test_postgres_plugin_attach(self, duckdb_connection):
        """
        Test to verify that the PostgreSQL database is attached correctly.
        """
        # Using "SHOW DATABASES" to check if the alias exists
        result = duckdb_connection.execute("SHOW DATABASES").fetchall()
        aliases = [row[0] for row in result]
        assert "postgres_db" in aliases, "Database 'postgres_db' should be attached."

    def test_postgres_plugin_read(self, duckdb_connection):
        """
        Test to verify reading data from the PostgreSQL table.
        """
        res = duckdb_connection.execute("SELECT SUM(i) FROM postgres_db.public.foo").fetchone()
        assert res[0] == 6, "The sum of 'i' should be 6."

    def test_postgres_plugin_write(self, duckdb_connection):
        """
        Test to verify writing data to the PostgreSQL database via DuckDB.
        """
        # Insert a record into 'foo'
        duckdb_connection.execute("INSERT INTO postgres_db.public.foo (id, i) VALUES (4, 4)")
        # Fetch the inserted record
        res = duckdb_connection.execute("SELECT i FROM postgres_db.public.foo WHERE id = 4").fetchone()
        assert res[0] == 4, "The value of 'i' should be 4."

    def test_postgres_plugin_read_only(self, duckdb_connection_factory, dsn):
        """
        Test to verify that write operations fail when read_only is True.
        """
        config = {
            "dsn": dsn,
            "duckdb_alias": "postgres_db_ro",
            "pg_schema": "public",
            "read_only": True,
        }
        conn = duckdb_connection_factory(plugin_config=config)

        # Try to perform a write operation and expect it to fail
        with pytest.raises(Exception) as exc_info:
            conn.execute("CREATE TABLE postgres_db_ro.public.bar (id INTEGER PRIMARY KEY, value TEXT)")
        assert "read-only" in str(exc_info.value).lower(), "Write operation should fail in read-only mode."
        conn.close()

    def test_postgres_plugin_extension_settings(self, duckdb_connection):
        """
        Test to verify that extension settings are applied correctly.
        """
        # Fetch the current setting of 'pg_use_binary_copy'
        res = duckdb_connection.execute("SELECT current_setting('pg_use_binary_copy');").fetchone()
        assert res[0] == True, "The setting 'pg_use_binary_copy' should be 'true'."

    def test_postgres_plugin_attach_options(self, duckdb_connection_factory, dsn):
        """
        Test to verify that additional ATTACH options are applied correctly.
        """
        dsn = (
            f"host={os.environ['DB_HOST']} "
            f"port={os.environ['DB_PORT']} "
            f"dbname={os.environ['DB_NAME']} "
            f"user={os.environ['DB_USERNAME']} "
            f"password={os.environ['DB_PASSWORD']}"
        )
        config = {
            "dsn": dsn,
            "duckdb_alias": "postgres_db_custom",
            "pg_schema": "public",
            "read_only": False,
            "settings": {  # Add this section
                "pg_null_byte_replacement": "?"
            }
        }

        conn = duckdb_connection_factory(plugin_config=config)

        # Verify that the database is attached with the custom alias
        result = conn.execute("SHOW DATABASES").fetchall()
        aliases = [row[0] for row in result]
        assert "postgres_db_custom" in aliases, "Database 'postgres_db_custom' should be attached."

        # Verify that the attach option is set
        res = conn.execute("SELECT current_setting('pg_null_byte_replacement');").fetchone()
        assert res[0] == "?", "The 'pg_null_byte_replacement' should be '?'."

        conn.close()

    def test_postgres_plugin_pg_array_as_varchar(self, duckdb_connection_factory, dsn):
        """
        Test to verify that 'pg_array_as_varchar' setting works correctly.
        """
        config = {
            "dsn": dsn,
            "duckdb_alias": "postgres_db_array",
            "pg_schema": "public",
            "read_only": False,
            "settings": {
                "pg_array_as_varchar": True
            }
        }

        conn = duckdb_connection_factory(plugin_config=config)

        # Create a table with an array column
        conn.execute("""
            CREATE TABLE postgres_db_array.public.array_table (
                id INTEGER PRIMARY KEY,
                arr INTEGER[]
            )
        """)
        conn.execute("INSERT INTO postgres_db_array.public.array_table (id, arr) VALUES (1, ARRAY[1,2,3])")

        # Fetch the array and expect a list
        res = conn.execute("SELECT arr FROM postgres_db_array.public.array_table WHERE id = 1").fetchone()
        assert res[0] == [1, 2, 3], "The array should be fetched as a list [1, 2, 3]."
        conn.close()


    def test_postgres_plugin_error_handling(self, duckdb_connection_factory, dsn):
        """
        Test to verify that invalid configurations raise appropriate errors.
        """
        config = {
            "duckdb_alias": "postgres_db_error",
            "pg_schema": "public",
            "read_only": False,
        }
        with pytest.raises(ValueError) as exc_info:
            duckdb_connection_factory(plugin_config=config)
        assert "'dsn' is a required argument" in str(exc_info.value), "Should raise ValueError for missing 'dsn'"

        # Invalid setting
        config = {
            "dsn": dsn,
            "duckdb_alias": "postgres_db_error",
            "pg_schema": "public",
            "read_only": False,
            "settings": {
                "invalid_setting": True
            }
        }
        with pytest.raises(Exception) as exc_info:
            conn = duckdb_connection_factory(plugin_config=config)
        assert "unrecognized configuration parameter" in str(exc_info.value).lower(), "Should raise exception for invalid setting"

