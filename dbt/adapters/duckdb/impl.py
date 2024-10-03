import os
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING

from dbt_common.contracts.constraints import ColumnLevelConstraint
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtInternalError
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column as BaseColumn
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.relation import Path
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.duckdb.column import DuckDBColumn
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.relation import DuckDBRelation
from dbt.adapters.duckdb.utils import TargetConfig
from dbt.adapters.duckdb.utils import TargetLocation
from dbt.adapters.sql import SQLAdapter


TEMP_SCHEMA_NAME = "temp_schema_name"
DEFAULT_TEMP_SCHEMA_NAME = "dbt_temp"

if TYPE_CHECKING:
    import agate


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager
    Column = DuckDBColumn
    Relation = DuckDBRelation

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    # can be overridden via the model config metadata
    _temp_schema_name = DEFAULT_TEMP_SCHEMA_NAME

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return cls.ConnectionManager.env().is_cancelable()

    def debug_query(self):
        self.execute("select 1 as id")

    @available
    def is_motherduck(self):
        return self.config.credentials.is_motherduck

    @available
    def convert_datetimes_to_strs(self, table: "agate.Table") -> "agate.Table":
        import agate

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
    def get_seed_file_path(self, model) -> str:
        return os.path.join(model["root_path"], model["original_file_path"])

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
    def store_relation(
        self,
        plugin_name: str,
        relation: DuckDBRelation,
        column_list: Sequence[BaseColumn],
        path: str,
        format: str,
        config: Any,
    ) -> None:
        target_config = TargetConfig(
            relation=relation,
            column_list=column_list,
            config=config,
            location=TargetLocation(path=path, format=format),
        )
        DuckDBConnectionManager.env().store_relation(plugin_name, target_config)

    @available
    def external_root(self) -> str:
        return self.config.credentials.external_root

    @available
    def get_binding_char(self):
        return DuckDBConnectionManager.env().get_binding_char()

    @available
    def catalog_comment(self, prefix):
        if DuckDBConnectionManager.env().supports_comments():
            return f"{prefix}.comment"
        else:
            return "''"

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

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[BaseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""

        # Taking advantage of yet another amazing DuckDB SQL feature right here: the
        # ability to DESCRIBE a query instead of a relation
        describe_sql = f"DESCRIBE ({sql})"
        _, cursor = self.connections.add_select_query(describe_sql)
        ret = []
        for row in cursor.fetchall():
            name, dtype = row[0], row[1]
            ret.append(DuckDBColumn.create(name, dtype))
        return ret

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        """Render the given constraint as DDL text. Should be overriden by adapters which need custom constraint
        rendering."""
        if constraint.type == ConstraintType.foreign_key:
            if constraint.to and constraint.to_columns:
                # TODO: this is a hack to get around a limitation in DuckDB around setting FKs
                # across databases.
                pieces = constraint.to.split(".")
                if len(pieces) > 2:
                    constraint_to = ".".join(pieces[1:])
                else:
                    constraint_to = constraint.to
                return f"references {constraint_to} ({', '.join(constraint.to_columns)})"
            else:
                return f"references {constraint.expression}"
        else:
            return super().render_column_constraint(constraint)

    def _clean_up_temp_relation_for_incremental(self, config):
        if self.is_motherduck() and hasattr(config, "model"):
            if "incremental" == config.model.get_materialization():
                temp_relation = self.Relation(
                    path=self.get_temp_relation_path(config.model), type=RelationType.Table
                )
                self.drop_relation(temp_relation)

    def pre_model_hook(self, config: Any) -> None:
        """A hook for getting the temp schema name from the model config.
        Cleans up the remote temporary table on MotherDuck before running
        an incremental model.
        """
        if hasattr(config, "model"):
            self._temp_schema_name = config.model.config.meta.get(
                TEMP_SCHEMA_NAME, self._temp_schema_name
            )
            self._clean_up_temp_relation_for_incremental(config)
        super().pre_model_hook(config)

    @available
    def get_temp_relation_path(self, model: Any):
        """This is a workaround to enable incremental models on MotherDuck because it
        currently doesn't support remote temporary tables. Instead we use a regular
        table that is dropped at the end of the incremental macro or post-model hook.
        """
        return Path(
            schema=self._temp_schema_name, database=model.database, identifier=model.identifier
        )

    def post_model_hook(self, config: Any, context: Any) -> None:
        """A hook for cleaning up the remote temporary table on MotherDuck if the
        incremental model materialization fails to do so.
        """
        self._clean_up_temp_relation_for_incremental(config)
        super().post_model_hook(config, context)


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
