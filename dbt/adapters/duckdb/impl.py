from typing import List
from typing import Optional
from typing import Sequence

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from dbt.adapters.base.meta import available
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.glue import create_or_update_table
from dbt.adapters.duckdb.relation import DuckDBRelation
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import InternalException
from dbt.exceptions import RuntimeException


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager
    Relation = DuckDBRelation

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @available
    def location_exists(self, location: str) -> bool:
        try:
            self.execute(
                f"select 1 from '{location}' where 1=0",
                auto_begin=False,
                fetch=False,
            )
            return True
        except RuntimeException:
            return False

    @available
    def register_glue_table(
        self,
        glue_database: str,
        table: str,
        column_list: Sequence[Column],
        location: str,
        file_format: str,
    ) -> None:
        create_or_update_table(
            database=glue_database,
            table=table,
            column_list=column_list,
            s3_path=location,
            file_format=file_format,
            settings=self.config.credentials.settings,
        )

    @available
    def external_root(self) -> str:
        return self.config.credentials.external_root

    def valid_incremental_strategies(self) -> Sequence[str]:
        """DuckDB does not currently support MERGE statement."""
        return ["append", "delete+insert"]

    def commit_if_has_connection(self) -> None:
        """This is just a quick-fix. Python models do not execute begin function so the transaction_open is always false."""
        try:
            self.connections.commit_if_has_connection()
        except InternalException:
            pass

    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:

        connection = self.connections.get_if_exists()
        if not connection:
            connection = self.connections.get_thread_connection()
        con = connection.handle._conn

        def load_df_function(table_name: str):
            """
            Currently con.table method dos not support fully qualified name - https://github.com/duckdb/duckdb/issues/5038

            Can be replaced by con.table, after it is fixed.
            """
            return con.query(f"select * from {table_name}")

        try:
            exec(compiled_code, {}, {"load_df_function": load_df_function, "con": con})
        except SyntaxError as err:
            raise RuntimeException(
                f"Python model has a syntactic error at line {err.lineno}:\n" f"{err}\n"
            )
        except Exception as err:
            raise RuntimeException(f"Python model failed:\n" f"{err}")
        return AdapterResponse(_message="OK")

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.

        This method only really exists for test reasons.
        """
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )
        return sql


# Change `table_a/b` to `table_aaaaa/bbbbb` to avoid duckdb binding issues when relation_a/b
# is called "table_a" or "table_b" in some of the dbt tests
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_aaaaa as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_bbbbb as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_aaaaa.num_rows - table_bbbbb.num_rows as difference
    from table_aaaaa, table_bbbbb
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
""".strip()
