from tests.ducklake import DUCKLAKE_DATABASE_TYPE


def test_requested_database_type(project, database_type, profile_type):
    "Sanity check that the target database is actually DuckLake."
    if database_type != DUCKLAKE_DATABASE_TYPE:
        return

    # DuckLake on motherduck has type = 'motherduck' on duckdb_databases()
    if profile_type == "md":
        assert project.adapter.config.credentials.is_ducklake is True
        return

    database_storage_type = project.run_sql(
        """
        select type
        from duckdb_databases()
        where database_name = current_database()
        """,
        fetch="one",
    )[0]
    assert database_storage_type.lower() == DUCKLAKE_DATABASE_TYPE
