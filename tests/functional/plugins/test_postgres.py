import os

import pytest
from testcontainers.postgres import PostgresContainer

from dbt.adapters.duckdb.plugins.postgres import Plugin
from dbt.adapters.events.logging import AdapterLogger
from dbt.tests.util import run_dbt, run_sql_with_adapter

logger = AdapterLogger("DuckDB_PostgresPlugin")


@pytest.mark.skip(
    reason="""
Skipping this b/c it requires running a properly setup Postgres server
when testing it locally and also b/c I think there is something
wrong with profiles_config_update since it can't be used in multiple
tests in the same pytest session

Exercise locally with: pytest --profile=file tests/functional/plugins/test_postgres.py
there are networking issues testing this in a devcontainer, more here https://github.com/testcontainers/testcontainers-python/issues/538
the test works outside the devcontainer though"""
)
@pytest.mark.usefixtures("postgres_container")
class TestPostgresPluginWithDbt:
    @pytest.fixture(scope="class")
    def postgres_container(self):
        """
        Fixture to set up a PostgreSQL container using Testcontainers.
        """
        postgres = PostgresContainer("postgres:16-alpine")
        postgres.start()

        # Set environment variables for the PostgreSQL connection
        os.environ["DB_HOST"] = postgres.get_container_host_ip()
        os.environ["DB_PORT"] = postgres.get_exposed_port(5432)
        os.environ["DB_USERNAME"] = postgres.username
        os.environ["DB_PASSWORD"] = postgres.password
        os.environ["DB_NAME"] = postgres.dbname

        # Log environment variables for debugging
        logger.info(f"DB_HOST={os.getenv('DB_HOST')}")
        logger.info(f"DB_PORT={os.getenv('DB_PORT')}")
        logger.info(f"DB_USERNAME={os.getenv('DB_USERNAME')}")
        logger.info(f"DB_PASSWORD={os.getenv('DB_PASSWORD')}")
        logger.info(f"DB_NAME={os.getenv('DB_NAME')}")

        yield postgres

        # Cleanup after tests
        postgres.stop()

    @pytest.fixture(scope="class", autouse=True)
    def attach_postgres_db(self, project, postgres_container):
        import time

        from psycopg import OperationalError, connect

        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        # Wait for the PostgreSQL container to be ready
        retries = 5
        while retries > 0:
            try:
                with connect(
                    host=db_host,
                    port=db_port,
                    user=db_username,
                    password=db_password,
                    dbname=db_name,
                ) as conn:
                    logger.info("PostgreSQL container is ready.")
                    break
            except OperationalError as e:
                logger.warning(f"PostgreSQL not ready, retrying... ({e})")
                retries -= 1
                time.sleep(2)
        else:
            raise RuntimeError("PostgreSQL container not ready after retries.")

        # Load DuckDB's postgres extension
        run_sql_with_adapter(project.adapter, "INSTALL postgres;")
        run_sql_with_adapter(project.adapter, "LOAD postgres;")

        # Build the DSN
        dsn = (
            f"host={db_host} "
            f"port={db_port} "
            f"dbname={db_name} "
            f"user={db_username} "
            f"password={db_password}"
        )

        # Attach the Postgres database
        attach_sql = f"""
            ATTACH '{dsn}' AS postgres_db (TYPE POSTGRES);
        """
        run_sql_with_adapter(project.adapter, attach_sql)

        # Create the 'foo' table in PostgreSQL via DuckDB connection
        create_table_sql = """
            CREATE TABLE postgres_db.public.foo (
                id INTEGER PRIMARY KEY,
                i INTEGER
            );
        """
        run_sql_with_adapter(project.adapter, create_table_sql)

        # Insert data into 'foo' table
        insert_sql = """
            INSERT INTO postgres_db.public.foo (id, i) VALUES (1, 2), (2, 2), (3, 2);
        """
        run_sql_with_adapter(project.adapter, insert_sql)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": """
                select * from postgres_db.public.foo
            """
        }

    def test_postgres_plugin_attach(self, project):
        """
        Test to verify that the PostgreSQL database is attached correctly.
        """
        # Run SQL to show databases
        result = run_sql_with_adapter(project.adapter, "SHOW DATABASES;", fetch="all")
        aliases = [row[0] for row in result]
        assert "postgres_db" in aliases, "Database 'postgres_db' should be attached."

    def test_postgres_plugin_read(self, project):
        """
        Test to verify reading data from the PostgreSQL table.
        """
        # Run dbt models
        results = run_dbt(["run"])
        assert len(results) > 0, "Expected at least one model to run successfully."

        # Get the test schema
        test_schema = project.test_schema

        # Verify the data using the fully qualified table name
        sum_i = run_sql_with_adapter(
            project.adapter, f"SELECT SUM(i) FROM {test_schema}.my_model;", fetch="one"
        )[0]
        assert sum_i == 6, "The sum of 'i' should be 6."

    def test_postgres_plugin_write(self, project):
        """
        Test to verify writing data to the PostgreSQL database via DuckDB.
        """
        # Insert a record into 'foo' via DuckDB
        insert_sql = """
            INSERT INTO postgres_db.public.foo (id, i) VALUES (4, 4);
        """
        run_sql_with_adapter(project.adapter, insert_sql)

        # Verify the inserted record
        res = run_sql_with_adapter(
            project.adapter,
            "SELECT i FROM postgres_db.public.foo WHERE id = 4;",
            fetch="one",
        )
        assert res[0] == 4, "The value of 'i' should be 4."

    def test_postgres_plugin_read_only(self, project):
        """
        Test to verify that write operations fail when read_only is True.
        """
        # Detach the existing postgres_db
        run_sql_with_adapter(project.adapter, "DETACH DATABASE postgres_db;")

        # Re-attach with read_only=True
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        dsn = (
            f"host={db_host} "
            f"port={db_port} "
            f"dbname={db_name} "
            f"user={db_username} "
            f"password={db_password}"
        )

        sql = f"""
            ATTACH '{dsn}' AS postgres_db_ro (TYPE POSTGRES, READ_ONLY);
        """
        run_sql_with_adapter(project.adapter, sql)

        # Try to perform a write operation and expect it to fail
        with pytest.raises(Exception) as exc_info:
            run_sql_with_adapter(
                project.adapter,
                """
                CREATE TABLE postgres_db_ro.public.bar (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                """,
            )
        assert (
            "read-only" in str(exc_info.value).lower()
        ), "Write operation should fail in read-only mode."

    def test_postgres_plugin_extension_settings(self, project):
        """
        Test to verify that extension settings are applied correctly.
        """
        # Set the extension setting
        run_sql_with_adapter(project.adapter, "SET pg_use_binary_copy = true;")

        # Verify the current setting of 'pg_use_binary_copy'
        res = run_sql_with_adapter(
            project.adapter,
            "SELECT current_setting('pg_use_binary_copy');",
            fetch="one",
        )
        assert res[0] == True, "The setting 'pg_use_binary_copy' should be 'true'."

    def test_postgres_plugin_attach_options(self, project):
        """
        Test to verify that additional ATTACH options are applied correctly.
        """
        # Detach any existing databases
        run_sql_with_adapter(project.adapter, "DETACH DATABASE IF EXISTS postgres_db;")
        run_sql_with_adapter(
            project.adapter, "DETACH DATABASE IF EXISTS postgres_db_custom;"
        )

        # Re-attach with a custom alias and additional settings
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        # Construct the DSN
        dsn = (
            f"host={db_host} "
            f"port={db_port} "
            f"dbname={db_name} "
            f"user={db_username} "
            f"password={db_password}"
        )

        # Attach the Postgres database with a custom alias
        attach_sql = f"""
            ATTACH '{dsn}' AS postgres_db_custom (TYPE POSTGRES);
        """
        run_sql_with_adapter(project.adapter, attach_sql)

        # Set the additional setting for pg_null_byte_replacement
        set_sql = "SET pg_null_byte_replacement = '?';"

        # Verify that the setting was applied
        validate_sql = "SELECT current_setting('pg_null_byte_replacement');"
        res = run_sql_with_adapter(project.adapter, set_sql + validate_sql, fetch="one")
        logger.info(f"pg_null_byte_replacement setting: {res[0]}")
        assert res[0] == "?", "The 'pg_null_byte_replacement' should be '?'."

        # Verify that the database is attached with the custom alias
        result = run_sql_with_adapter(project.adapter, "SHOW DATABASES;", fetch="all")
        aliases = [row[0] for row in result]
        assert (
            "postgres_db_custom" in aliases
        ), "Database 'postgres_db_custom' should be attached."

        # Detach the database after validation
        run_sql_with_adapter(project.adapter, "DETACH DATABASE postgres_db_custom;")

    def test_postgres_plugin_pg_array_as_varchar(self, project):
        """
        Test to verify that 'pg_array_as_varchar' setting works correctly.
        """
        # Detach the existing postgres_db
        run_sql_with_adapter(project.adapter, "DETACH DATABASE IF EXISTS postgres_db;")

        # Re-attach without SETTINGS
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        dsn = (
            f"host={db_host} "
            f"port={db_port} "
            f"dbname={db_name} "
            f"user={db_username} "
            f"password={db_password}"
        )

        sql = f"""
            ATTACH '{dsn}' AS postgres_db_array (TYPE POSTGRES);
        """
        run_sql_with_adapter(project.adapter, sql)

        # Set the 'pg_array_as_varchar' setting
        set_sql = "SET pg_array_as_varchar = true;"
        run_sql_with_adapter(project.adapter, set_sql)

        # Create a table with an array column
        create_table_sql = """
            CREATE TABLE postgres_db_array.public.array_table (
                id INTEGER PRIMARY KEY,
                arr INTEGER[]
            );
        """
        run_sql_with_adapter(project.adapter, create_table_sql)

        # Insert data into the array_table
        insert_sql = """
            INSERT INTO postgres_db_array.public.array_table (id, arr) VALUES (1, ARRAY[1,2,3]);
        """
        run_sql_with_adapter(project.adapter, insert_sql)

        # Fetch the array and expect a list
        res = run_sql_with_adapter(
            project.adapter,
            "SELECT arr FROM postgres_db_array.public.array_table WHERE id = 1;",
            fetch="one",
        )
        assert res[0] == [1, 2, 3], "The array should be fetched as a list [1, 2, 3]."

    def test_postgres_plugin_error_handling(self, project):
        """
        Test to verify that invalid configurations raise appropriate errors.
        """
        # Try to attach without a DSN
        with pytest.raises(Exception) as exc_info:
            run_sql_with_adapter(
                project.adapter, "ATTACH '' AS postgres_db_error (TYPE POSTGRES);"
            )
        assert (
            "unable to connect to postgres" in str(exc_info.value).lower()
        ), "Should raise exception for missing 'dsn'"

        # Provide a valid DSN for the next test
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        dsn = (
            f"host={db_host} "
            f"port={db_port} "
            f"dbname={db_name} "
            f"user={db_username} "
            f"password={db_password}"
        )

        # Try to set an invalid setting
        with pytest.raises(Exception) as exc_info:
            run_sql_with_adapter(
                project.adapter,
                f"""
                ATTACH '{dsn}' AS postgres_db_error (TYPE POSTGRES);
                SET invalid_setting = 'true';
                """,
            )
        assert (
            "unrecognized configuration parameter" in str(exc_info.value).lower()
        ), "Should raise syntax error for invalid setting"
