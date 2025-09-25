import os
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING
from uuid import uuid4

from dbt_common.contracts.constraints import ColumnLevelConstraint
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.dataclass_schema import ValidationError
from dbt_common.exceptions import DbtDatabaseError
from dbt_common.exceptions import DbtInternalError
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import encoding as dbt_encoding
from packaging.version import Version

from .constants import DEFAULT_TEMP_SCHEMA_NAME
from .constants import DUCKDB_BASE_INCREMENTAL_STRATEGIES
from .constants import DUCKDB_MERGE_LOWEST_VERSION_POSSIBLE
from .constants import TEMP_SCHEMA_NAME
from dbt.adapters.base import AdapterConfig
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
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import IndexConfigError
from dbt.adapters.exceptions import IndexConfigNotDictError
from dbt.adapters.sql import SQLAdapter


if TYPE_CHECKING:
    import agate

logger = AdapterLogger("DuckDB")


@dataclass
class DuckDBIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.utcnow().isoformat()
        inputs = self.columns + [
            relation.render(),
            str(self.unique),
            now,
        ]
        string = "_".join(inputs)
        return dbt_encoding.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["DuckDBIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass
class DuckDBConfig(AdapterConfig):
    indexes: Optional[List[DuckDBIndexConfig]] = None


class DuckDBAdapter(SQLAdapter):
    ConnectionManager = DuckDBConnectionManager
    Column = DuckDBColumn
    Relation = DuckDBRelation

    AdapterSpecificConfigs = DuckDBConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    # can be overridden via the model config metadata
    _temp_schema_name = DEFAULT_TEMP_SCHEMA_NAME
    _temp_schema_model_uuid: dict[str, str] = defaultdict(lambda: str(uuid4()).split("-")[-1])

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return cls.ConnectionManager.env().is_cancelable()

    def debug_query(self):
        self.execute("select 1 as id")

    @available
    def parse_index(self, raw_index: Any) -> Optional[DuckDBIndexConfig]:
        return DuckDBIndexConfig.parse(raw_index)

    @available
    def is_motherduck(self):
        return self.config.credentials.is_motherduck

    @available
    def disable_transactions(self):
        return self.config.credentials.disable_transactions

    @available
    def is_ducklake(self, relation: DuckDBRelation) -> bool:
        """Check if a relation's database is backed by a ducklake attachment."""
        if not relation or not relation.database:
            return False

        return relation.database in self.config.credentials._ducklake_dbs

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
        if rendered_options.get("partition_by") or rendered_options.get("per_thread_output"):
            globs = [write_location, "*"]
            if rendered_options.get("partition_by"):
                partition_by = str(rendered_options.get("partition_by"))
                globs.extend(["*"] * len(partition_by.split(",")))
            return ".".join(["/".join(globs), str(rendered_options.get("format", "parquet"))])
        return write_location

    @available
    def warn_once(self, msg: str):
        """Post a warning message once per dbt execution."""
        DuckDBConnectionManager.warn_once(msg)

    @cached_property
    def duckdb_version(self) -> Version:
        """Get the DuckDB version for the current DuckDB connection (cached)."""
        _, cursor = self.connections.add_select_query("SELECT version()")
        row = cursor.fetchone()
        version_string = row[0] if row else None

        if not version_string:
            raise DbtDatabaseError(
                "Unable to determine DuckDB version: version() query returned no results"
            )

        return Version(version_string)

    @cached_property
    def duckdb_incremental_strategies(self) -> Sequence[str]:
        """Return valid incremental strategies for the current DuckDB connection (cached)."""
        if self.duckdb_version >= Version(DUCKDB_MERGE_LOWEST_VERSION_POSSIBLE):
            return DUCKDB_BASE_INCREMENTAL_STRATEGIES + ["merge"]

        return DUCKDB_BASE_INCREMENTAL_STRATEGIES

    def valid_incremental_strategies(self) -> Sequence[str]:
        """Return valid incremental strategies for the current DuckDB connection."""
        return self.duckdb_incremental_strategies

    @available.parse_none
    def get_incremental_strategy_macro(self, model_context, strategy: str):
        if strategy == "merge" and self.duckdb_version < Version(
            DUCKDB_MERGE_LOWEST_VERSION_POSSIBLE
        ):
            raise DbtRuntimeError(
                f"The 'merge' incremental strategy requires DuckDB >= {DUCKDB_MERGE_LOWEST_VERSION_POSSIBLE}. "
                f"Current version: {self.duckdb_version}. "
                f"Please upgrade DuckDB or use 'append' or 'delete+insert'."
            )

        return super().get_incremental_strategy_macro(model_context, strategy)

    def commit_if_has_connection(self) -> None:
        """This is just a quick-fix. Python models do not execute begin function so the transaction_open is always false."""
        try:
            self.connections.commit_if_has_connection()
        except DbtInternalError as e:
            # Log commit errors instead of silently swallowing them to aid debugging
            logger.warning(f"Commit failed with DbtInternalError: {e}\n{traceback.format_exc()}")
            # Still pass to maintain backward compatibility, but now with visibility
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
    def get_column_schema_from_query(self, sql: str) -> List[DuckDBColumn]:
        """Get a list of the column names and data types from the given sql.

        :param str sql: The sql to execute.
        :return: List[DuckDBColumn]
        """
        # Taking advantage of yet another amazing DuckDB SQL feature right here: the
        # ability to DESCRIBE a query instead of a relation
        describe_sql = f"DESCRIBE ({sql})"
        _, cursor = self.connections.add_select_query(describe_sql)
        flattened_columns = []
        for row in cursor.fetchall():
            name, dtype = row[0], row[1]
            column = DuckDBColumn(column=name, dtype=dtype)
            flattened_columns.extend(column.flatten())
        return flattened_columns

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
                    path=self.get_temp_relation_path(config.model),
                    type=RelationType.Table,
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
        # Add a unique identifier for this model (scoped per dbt run)
        _uuid = self._temp_schema_model_uuid[model.identifier]
        return Path(
            schema=self._temp_schema_name,
            database=model.database,
            identifier=f"{model.identifier}__{_uuid}",
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
