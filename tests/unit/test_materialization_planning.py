from dbt.adapters.duckdb.impl import DuckDBAdapter
from dbt.adapters.duckdb.relation import DuckDBRelation
from dbt.adapters.planning import (
    ExistingIndexStrategy,
    TableDocumentationStrategy,
    TableIndexStrategy,
    TableReplacementStrategy,
)


def _adapter() -> DuckDBAdapter:
    return object.__new__(DuckDBAdapter)


def test_duckdb_sql_table_resolves_stage_and_swap_index_policies() -> None:
    plan = DuckDBAdapter.plan_table_materialization(
        _adapter(),
        "macro.dbt_duckdb.materialization_table_duckdb",
        "sql",
    )

    assert plan.replacement == TableReplacementStrategy.STAGE_AND_SWAP
    assert plan.indexes == TableIndexStrategy.AFTER_SWAP
    assert plan.existing_indexes == ExistingIndexStrategy.DROP_BEFORE_SWAP


def test_ducklake_runtime_facts_refine_docs_and_statement_boundaries() -> None:
    adapter = _adapter()
    adapter.is_ducklake = lambda relation: True
    relation = DuckDBRelation.create(
        database="lake",
        schema="mart",
        identifier="orders",
        type="table",
    )
    plan = DuckDBAdapter.plan_table_materialization(
        adapter,
        "macro.dbt_duckdb.materialization_table_duckdb",
        "sql",
    )

    resolved = DuckDBAdapter.resolve_table_lifecycle_plan(
        adapter,
        plan,
        object(),
        relation,
        {},
    )

    assert resolved.documentation == TableDocumentationStrategy.AFTER_COMMIT


def test_storage_configured_table_stays_on_jinja_compatibility_path() -> None:
    model = type("Model", (), {"config": {"partitioned_by": ["ordered_at"]}})()

    plan = DuckDBAdapter.plan_table_materialization(
        _adapter(),
        "macro.dbt_duckdb.materialization_table_duckdb",
        "sql",
        model,
    )

    assert plan is None


def test_plain_duckdb_keeps_base_runtime_policies() -> None:
    adapter = _adapter()
    adapter.is_ducklake = lambda relation: False
    relation = DuckDBRelation.create(identifier="orders", type="table")
    plan = DuckDBAdapter.plan_table_materialization(
        adapter,
        "macro.dbt_duckdb.materialization_table_duckdb",
        "sql",
    )

    resolved = DuckDBAdapter.resolve_table_lifecycle_plan(
        adapter,
        plan,
        object(),
        relation,
        {},
    )

    assert resolved is plan
