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
