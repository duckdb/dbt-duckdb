import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
)
from dbt.tests.util import run_dbt
from dbt.tests.util import run_dbt_and_capture


pytestmark = pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support primary or unique constraints"
)


class DuckDBColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "TIMESTAMP", "TIMESTAMP"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"],
            ["ARRAY['a','b','c']", "VARCHAR[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "INTEGER[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                "json",
                "JSON",
            ],
        ]


class TestTableConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass


@pytest.mark.skip_profile("md")
class TestTableConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement
):
    pass


@pytest.mark.skip_profile("md", "buenavista")
class TestTableConstraintsRollback(DuckDBColumnEqualSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md")
class TestIncrementalConstraintsRuntimeDdlEnforcement(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md", "buenavista")
class TestIncrementalConstraintsRollback(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsRollback
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md")
class TestModelConstraintsRuntimeEnforcement(
    DuckDBColumnEqualSetup, BaseModelConstraintsRuntimeEnforcement
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip_profile("md", "buenavista")
class TestCompositeModelLevelForeignKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": "select 1 as id, 'parent' as type",
            "child.sql": "select 1 as parent_id, 'parent' as parent_type",
            "schema.yml": """
version: 2
models:
  - name: parent
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, type]
    columns:
      - name: id
        data_type: integer
      - name: type
        data_type: varchar
  - name: child
    config:
      contract:
        enforced: true
    constraints:
      - type: foreign_key
        columns: [parent_id, parent_type]
        to: ref('parent')
        to_columns: [id, type]
    columns:
      - name: parent_id
        data_type: integer
      - name: parent_type
        data_type: varchar
""",
        }

    def test_composite_model_level_foreign_key(self, project):
        results = run_dbt(["run"])

        assert len(results) == 2


@pytest.mark.skip_profile("md", "buenavista")
class TestContractQuotedColumnNames:
    @pytest.fixture(scope="class")
    def models(self):
        model_sql = """select 1 as "Order ID", 'a' as "group", 'x' as plain_col"""
        columns_yml = """
    columns:
      - name: Order ID
        data_type: integer
        quote: true
        constraints:
          - type: not_null
      - name: group
        data_type: varchar
        quote: true
      - name: plain_col
        data_type: varchar
"""
        return {
            "quoted_table.sql": model_sql,
            "quoted_incremental.sql": model_sql,
            "schema.yml": f"""
version: 2
models:
  - name: quoted_table
    config:
      materialized: table
      contract:
        enforced: true
{columns_yml}
  - name: quoted_incremental
    config:
      materialized: incremental
      on_schema_change: append_new_columns
      contract:
        enforced: true
{columns_yml}
""",
        }

    def test_contract_quoted_column_names(self, project):
        # The second run exercises the incremental merge path on top of the
        # contracted table built by the first run.
        for _ in range(2):
            results = run_dbt(["run"])
            assert len(results) == 2

        table_rows = project.run_sql(
            f'select "Order ID", "group", plain_col from {project.test_schema}.quoted_table',
            fetch="all",
        )
        assert table_rows == [(1, "a", "x")]

        incremental_rows = project.run_sql(
            f'select "Order ID", "group", plain_col from {project.test_schema}.quoted_incremental',
            fetch="all",
        )
        assert incremental_rows == [(1, "a", "x"), (1, "a", "x")]


fk_parent_sql = """
{{ config(materialized='table') }}
select {{ var('parent_id', 1) }} as id
"""

fk_child_sql = """
{{ config(materialized='table') }}
select 10 as id, 1 as foo_id, 1 as bar_id
{% if var('invalid_child', false) %}
union all select 11 as id, 2 as foo_id, 1 as bar_id
{% endif %}
"""

fk_grandchild_sql = """
-- depends_on: {{ ref('child') }}
{{ config(materialized='table') }}
select 10 as child_id
"""

fk_schema_yml = """
version: 2

models:
  - name: foo
    config:
      contract: {enforced: true}
    columns:
      - name: id
        data_type: int
        constraints: [{type: primary_key}]
  - name: bar
    config:
      contract: {enforced: true}
    columns:
      - name: id
        data_type: int
        constraints: [{type: primary_key}]
  - name: child
    config:
      contract: {enforced: true}
    columns:
      - name: id
        data_type: int
        constraints: [{type: primary_key}]
      - name: foo_id
        data_type: int
        constraints: [{type: foreign_key, to: ref('foo'), to_columns: [id]}]
      - name: bar_id
        data_type: int
        constraints: [{type: foreign_key, to: ref('bar'), to_columns: [id]}]
  - name: grandchild
    config:
      contract: {enforced: true}
    columns:
      - name: child_id
        data_type: int
        constraints: [{type: foreign_key, expression: "{schema}.child (id)"}]
"""


@pytest.mark.skip_profile("md", "buenavista")
class TestForeignKeyRerun:
    """Re-running models that a foreign key references (issue #425)."""

    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        return {
            "foo.sql": fk_parent_sql,
            "bar.sql": fk_parent_sql,
            "child.sql": fk_child_sql,
            "grandchild.sql": fk_grandchild_sql,
            "schema.yml": fk_schema_yml.replace("{schema}", unique_schema),
        }

    def assert_all_tables_droppable(self, project):
        # DuckDB keeps the name a foreign key table was created under on the table it
        # references, so a table that was renamed into place leaves its parent impossible
        # to drop (duckdb/duckdb#24648). Dropping everything checks we never do that.
        for name in ["grandchild", "child", "foo", "bar"]:
            project.run_sql(f"drop table {project.test_schema}.{name}")

    def test_rerun(self, project):
        for _ in range(3):
            results = run_dbt(["run"])
            assert len(results) == 4
        # the dependents were rebuilt, with their foreign keys
        fks = project.run_sql(
            f"""
            select table_name, referenced_table from duckdb_constraints()
            where constraint_type = 'FOREIGN KEY' and schema_name = '{project.test_schema}'
            order by all
            """,
            fetch="all",
        )
        assert [tuple(row) for row in fks] == [
            ("child", "bar"),
            ("child", "foo"),
            ("grandchild", "child"),
        ]
        self.assert_all_tables_droppable(project)

    def test_rerun_with_threads(self, project):
        for _ in range(3):
            results = run_dbt(["run", "--threads", "8"])
            assert len(results) == 4
        self.assert_all_tables_droppable(project)

    def test_rerun_dependent_only(self, project):
        run_dbt(["run"])
        run_dbt(["run", "-s", "grandchild"])
        self.assert_all_tables_droppable(project)

    def test_unselected_dependent_is_an_error(self, project):
        run_dbt(["run"])
        _, log = run_dbt_and_capture(["run", "-s", "foo"], expect_pass=False)
        assert "child (model child is not selected in this run)" in log
        assert "grandchild (model grandchild is not selected in this run)" in log
        # nothing was dropped
        assert len(project.run_sql(f"select * from {project.test_schema}.grandchild", fetch="all")) == 1

    def test_failed_child_rebuild_rolls_back(self, project):
        run_dbt(["run"])
        run_dbt(
            ["run", "-s", "child", "--vars", "{invalid_child: true}"],
            expect_pass=False,
        )
        rows = project.run_sql(
            f"select * from {project.test_schema}.child",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [(10, 1, 1)]
        self.assert_all_tables_droppable(project)

    def test_failed_child_rebuild_after_parent_preserves_dependents(self, project):
        run_dbt(["run"])
        run_dbt(["run", "--vars", "{invalid_child: true}"], expect_pass=False)
        child_rows = project.run_sql(
            f"select * from {project.test_schema}.child",
            fetch="all",
        )
        grandchild_rows = project.run_sql(
            f"select * from {project.test_schema}.grandchild",
            fetch="all",
        )
        assert [tuple(row) for row in child_rows] == [(10, 1, 1)]
        assert [tuple(row) for row in grandchild_rows] == [(10,)]
        self.assert_all_tables_droppable(project)


@pytest.mark.skip_profile("md", "buenavista")
class TestForeignKeyRemoval:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": fk_parent_sql,
            "child.sql": """
-- depends_on: {{ ref('foo') }}
{{ config(materialized='table') }}
select 1 as foo_id
""",
            "schema.yml": """
version: 2
models:
  - name: foo
    config:
      contract: {enforced: true}
    columns:
      - {name: id, data_type: int, constraints: [{type: primary_key}]}
  - name: child
    config:
      contract: {enforced: true}
    columns:
      - name: foo_id
        data_type: int
        constraints: [{type: foreign_key, to: ref('foo'), to_columns: [id]}]
""",
        }

    def test_removing_foreign_key_does_not_leave_stale_reference(self, project):
        run_dbt(["run"])
        with open(f"{project.project_root}/models/schema.yml", "w") as schema_file:
            schema_file.write(
                """
version: 2
models:
  - name: foo
    config:
      contract: {enforced: true}
    columns:
      - {name: id, data_type: int, constraints: [{type: primary_key}]}
  - name: child
    config:
      contract: {enforced: true}
    columns:
      - {name: foo_id, data_type: int}
"""
            )
        run_dbt(["run", "-s", "child"])
        project.run_sql(f"drop table {project.test_schema}.child")
        project.run_sql(f"drop table {project.test_schema}.foo")


@pytest.mark.skip_profile("md", "buenavista")
class TestForeignKeySharedDependentFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": fk_parent_sql,
            "bar.sql": """
{{ config(materialized='table') }}
select
  {% if var('fail_bar', false) %}cast('invalid' as int){% else %}id{% endif %} as id
from {{ ref('foo') }}
""",
            "child.sql": """
{{ config(materialized='table') }}
select 1 as foo_id, 1 as bar_id
""",
            "schema.yml": """
version: 2
models:
  - name: foo
    config:
      contract: {enforced: true}
    columns:
      - {name: id, data_type: int, constraints: [{type: primary_key}]}
  - name: bar
    config:
      contract: {enforced: true}
    columns:
      - {name: id, data_type: int, constraints: [{type: primary_key}]}
  - name: child
    config:
      contract: {enforced: true}
    columns:
      - name: foo_id
        data_type: int
        constraints: [{type: foreign_key, to: ref('foo'), to_columns: [id]}]
      - name: bar_id
        data_type: int
        constraints: [{type: foreign_key, to: ref('bar'), to_columns: [id]}]
""",
        }

    def test_later_parent_failure_preserves_shared_dependent(self, project):
        run_dbt(["run"])
        run_dbt(["run", "--vars", "{fail_bar: true}"], expect_pass=False)
        tables = project.run_sql(
            f"""
            select table_name
            from duckdb_tables()
            where schema_name = '{project.test_schema}'
            """,
            fetch="all",
        )
        assert ("child",) in [tuple(row) for row in tables]
        rows = project.run_sql(
            f"select * from {project.test_schema}.child",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [(1, 1)]
        run_dbt(["run"])
        foreign_keys = project.run_sql(
            f"""
            select referenced_table
            from duckdb_constraints()
            where schema_name = '{project.test_schema}'
              and table_name = 'child'
              and constraint_type = 'FOREIGN KEY'
            """,
            fetch="all",
        )
        assert {row[0] for row in foreign_keys} == {"foo", "bar"}


@pytest.mark.skip_profile("md", "buenavista")
class TestForeignKeyDependentWithoutRef:
    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        return {
            "foo.sql": fk_parent_sql,
            "child.sql": "{{ config(materialized='table') }} select 1 as foo_id",
            "schema.yml": """
version: 2
models:
  - name: foo
    config:
      contract: {enforced: true}
    columns:
      - {name: id, data_type: int, constraints: [{type: primary_key}]}
  - name: child
    config:
      contract: {enforced: true}
    columns:
      - name: foo_id
        data_type: int
        constraints: [{type: foreign_key, expression: "{schema}.foo (id)"}]
""".replace("{schema}", unique_schema),
        }

    def test_dependent_without_ref_is_an_error(self, project):
        # without a ref() dbt is free to build child before foo
        run_dbt(["run", "-s", "foo"])
        run_dbt(["run", "-s", "child"])
        _, log = run_dbt_and_capture(["run", "-s", "foo", "child"], expect_pass=False)
        assert "does not ref() the table its foreign key points at" in log


@pytest.mark.skip_profile("md", "buenavista")
class TestForeignKeyIncrementalDependent:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": fk_parent_sql,
            "child.sql": """
{{ config(materialized='incremental', on_schema_change='fail') }}
select 1 as foo_id
""",
            "schema.yml": """
version: 2
models:
  - name: foo
    config:
      contract: {enforced: true}
    columns:
      - {name: id, data_type: int, constraints: [{type: primary_key}]}
  - name: child
    config:
      contract: {enforced: true}
    columns:
      - name: foo_id
        data_type: int
        constraints: [{type: foreign_key, to: ref('foo'), to_columns: [id]}]
""",
        }

    def test_incremental_dependent(self, project):
        run_dbt(["run"])
        _, log = run_dbt_and_capture(["run"], expect_pass=False)
        assert "is materialized as incremental and would lose its data" in log
        for _ in range(2):
            run_dbt(["run", "--full-refresh"])
        for name in ["child", "foo"]:
            project.run_sql(f"drop table {project.test_schema}.{name}")


fk_failing_child_sql = """
{{ config(materialized='table') }}
select {{ var('child_id', 10) }} as id,
  {% if var('fail', false) %}cast(error('boom') as int){% else %}1{% endif %} as foo_id,
  1 as bar_id
"""


@pytest.mark.skip_profile("md", "buenavista")
class TestForeignKeyFailedRebuildWithoutTransactions:
    """A table with a foreign key is dropped and recreated in place, so the rollback
    is the only thing that keeps its old rows if the build fails. That has to hold
    even when disable_transactions is set."""

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        target = {**dbt_profile_target, "schema": unique_schema, "disable_transactions": True}
        return {
            "test": {
                "outputs": {"dev": target},
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": fk_parent_sql,
            "bar.sql": fk_parent_sql,
            "child.sql": fk_failing_child_sql,
            "schema.yml": fk_schema_yml.split("  - name: grandchild")[0],
        }

    def test_failed_rebuild_keeps_old_rows(self, project):
        run_dbt(["run", "--vars", "{child_id: 7}"])
        _, log = run_dbt_and_capture(["run", "-s", "child", "--vars", "{fail: true}"], expect_pass=False)
        assert "disable_transactions is set, but dbt-duckdb uses a transaction" in log
        rows = project.run_sql(f"select id from {project.test_schema}.child", fetch="all")
        assert [tuple(row) for row in rows] == [(7,)]
        # and the next run still works
        run_dbt(["run"])
        rows = project.run_sql(f"select id from {project.test_schema}.child", fetch="all")
        assert [tuple(row) for row in rows] == [(10,)]
