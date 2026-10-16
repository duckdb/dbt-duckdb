def test_requested_database_type(project, database_type, profile_type):
    """
    Sanity check the physical and adapter-level target database types.

    DuckLake databases are declared differently on MotherDuck targets than on
    local DuckDB targets.
    """
    database_name = project.run_sql("select current_database()", fetch="one")[0]
    relation = project.adapter.Relation.create(
        database=database_name,
        schema="main",
        identifier="test_relation",
    )

    assert project.adapter.is_ducklake(relation) is (database_type == "ducklake")

    if database_type != "ducklake" or profile_type == "md":
        return

    database_storage_type = project.run_sql(
        """
        select type
        from duckdb_databases()
        where database_name = current_database()
        """,
        fetch="one",
    )[0]
    assert database_storage_type.lower() == "ducklake"
