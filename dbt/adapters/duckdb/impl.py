import os
from typing import List
from typing import Optional
from typing import Sequence

import agate
import duckdb

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from dbt.adapters.base.meta import available
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.glue import create_or_update_table
from dbt.adapters.duckdb.relation import DuckDBRelation
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import DbtInternalError
from dbt.exceptions import DbtRuntimeError


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
    def convert_datetimes_to_strs(self, table: agate.Table) -> agate.Table:
        for column in table.columns:
            if isinstance(column.data_type, agate.DateTime):
                table = table.compute(
                    [
                        (
                            column.name,
                            agate.Formula(agate.Text(), lambda row: str(row[column.name])),
                        )
                    ],
                    replace=True,
                )
        return table

    @available
    def location_exists(self, location: str) -> bool:
        try:
            self.execute(
                f"select 1 from '{location}' where 1=0",
                auto_begin=False,
                fetch=False,
            )
            return True
        except DbtRuntimeError:
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

    @available
    def use_database(self) -> bool:
        return duckdb.__version__ >= "0.7.0"

    @available
    def get_binding_char(self):
        return DuckDBConnectionManager.env().get_binding_char()

    @available
    def external_write_options(self, write_location: str, rendered_options: dict) -> str:
        if "format" not in rendered_options:
            ext = os.path.splitext(write_location)[1].lower()
            if ext:
                rendered_options["format"] = ext[1:]
            elif "delimiter" in rendered_options:
                rendered_options["format"] = "csv"
            else:
                rendered_options["format"] = "parquet"

        if rendered_options["format"] == "csv":
            if "header" not in rendered_options:
                rendered_options["header"] = 1

        if "partition_by" in rendered_options:
            v = rendered_options["partition_by"]
            if "," in v and not v.startswith("("):
                rendered_options["partition_by"] = f"({v})"

        ret = []
        for k, v in rendered_options.items():
            if k.lower() in {
                "delimiter",
                "quote",
                "escape",
                "null",
            } and not v.startswith("'"):
                ret.append(f"{k} '{v}'")
            else:
                ret.append(f"{k} {v}")
        return ", ".join(ret)

    @available
    def external_read_location(self, write_location: str, rendered_options: dict) -> str:
        if rendered_options.get("partition_by"):
            globs = [write_location, "*"]
            partition_by = str(rendered_options.get("partition_by"))
            globs.extend(["*"] * len(partition_by.split(",")))
            return ".".join(["/".join(globs), str(rendered_options.get("format", "parquet"))])
        return write_location

    def valid_incremental_strategies(self) -> Sequence[str]:
        """DuckDB does not currently support MERGE statement."""
        return ["append", "delete+insert"]

    def commit_if_has_connection(self) -> None:
        """This is just a quick-fix. Python models do not execute begin function so the transaction_open is always false."""
        try:
            self.connections.commit_if_has_connection()
        except DbtInternalError:
            pass

    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        connection = self.connections.get_if_exists()
        if not connection:
            connection = self.connections.get_thread_connection()
        env = DuckDBConnectionManager.env()
        return env.submit_python_job(connection.handle, parsed_model, compiled_code)

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
