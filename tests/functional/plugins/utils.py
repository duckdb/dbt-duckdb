
def get_table_row_count(dbt_project, full_table_name: str):
    """Get the row count of a table."""
    count_result, *_ = dbt_project.run_sql(f"SELECT COUNT(*) FROM {full_table_name}", fetch="all")
    row_count, *_ = count_result
    return row_count
