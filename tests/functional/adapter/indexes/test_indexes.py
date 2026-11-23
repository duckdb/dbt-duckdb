import pytest
import re
from dbt.tests.util import (
    relation_from_name,
    run_dbt,
)
from tests.functional.adapter.indexes.fixtures import (
    models__incremental_sql,
    models__incremental_sync_all_columns_add_sql,
    models__incremental_sync_all_columns_failing_sql,
    models__incremental_sync_all_columns_failing_type_change_sql,
    models__incremental_sync_all_columns_remove_sql,
    models__incremental_sync_all_columns_remove_unique_sql,
    models__incremental_sync_all_columns_type_change_sql,
    models__table_sql,
    seeds__seed_csv,
    snapshots__colors_sql,
)


INDEX_DEFINITION_PATTERN = re.compile(r"\((.*?)\)")


@pytest.mark.skip_database_type("ducklake", reason="DuckLake does not support indexes")
class TestIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": models__table_sql,
            "incremental.sql": models__incremental_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"colors.sql": snapshots__colors_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
                "indexes": [
                    {"columns": ["country_code"], "unique": False},
                    {"columns": ["country_code", "country_name"], "unique": True},
                ],
            },
            "vars": {
                "version": 1,
            },
        }

    def test_table(self, project, unique_schema):
        for _ in range(2):
            results = run_dbt(["run", "--models", "table"])
            assert len(results) == 1

            indexes = self.get_indexes("table", project, unique_schema)
            expected = [
                {"columns": "column_a", "unique": False},
                {"columns": "column_b", "unique": False},
                {"columns": "column_a, column_b", "unique": False},
                {"columns": "column_b, column_a", "unique": True},
                {"columns": "column_a", "unique": False},
            ]
            assert len(indexes) == len(expected)

    def test_incremental(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "incremental"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("incremental", project, unique_schema)
            expected = [
                {"columns": "column_a", "unique": False},
                {"columns": "column_a, column_b", "unique": True},
            ]
            assert len(indexes) == len(expected)

            # Regression for #660: full-refresh must clean up the __dbt_backup table.
            backup_tables = project.run_sql(
                f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE lower(table_schema) = lower('{unique_schema}')
                  AND table_name LIKE '%__dbt_backup'
                """,
                fetch="all",
            )
            assert backup_tables == [], f"__dbt_backup tables leaked after run: {backup_tables}"

    def test_seed(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["seed"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("seed", project, unique_schema)
            expected = [
                {"columns": "country_code", "unique": False},
                {
                    "columns": "country_code, country_name",
                    "unique": True,
                },
            ]
            assert len(indexes) == len(expected)

    def test_snapshot(self, project, unique_schema):
        for version in [1, 2]:
            results = run_dbt(["snapshot", "--vars", f"version: {version}"])
            assert len(results) == 1

            indexes = self.get_indexes("colors", project, unique_schema)
            expected = [
                {"columns": "id", "unique": False},
                {"columns": "id, color", "unique": True},
            ]
            assert len(indexes) == len(expected)

    def get_indexes(self, table_name, project, unique_schema):
        sql = f"""
            SELECT
              sql as index_definition, is_unique
            FROM duckdb_indexes()
            WHERE
              schema_name = '{unique_schema}'
              AND 
              table_name = '{table_name}'
        """
        results = project.run_sql(sql, fetch="all")
        return [self.parse_index_definition(row[0], row[1]) for row in results]

    def parse_index_definition(self, index_definition, is_unique):
        index_definition = index_definition.lower()
        m = INDEX_DEFINITION_PATTERN.search(index_definition)
        return {
            "columns": m.group(1),
            "unique": is_unique,
        }


@pytest.mark.skip_database_type("ducklake", reason="DuckLake does not support indexes")
class TestIncrementalIndexesOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "inc_indexed_remove.sql": models__incremental_sync_all_columns_remove_sql,
            "inc_indexed_remove_unique.sql": models__incremental_sync_all_columns_remove_unique_sql,
            "inc_indexed_add.sql": models__incremental_sync_all_columns_add_sql,
            "inc_indexed_failing_sync.sql": models__incremental_sync_all_columns_failing_sql,
            "inc_indexed_failing_type_change.sql": models__incremental_sync_all_columns_failing_type_change_sql,
            "inc_indexed_type_change.sql": models__incremental_sync_all_columns_type_change_sql,
        }

    def test_remove_column_keeps_surviving_indexes(self, project, unique_schema):
        run_dbt(["run", "--models", "inc_indexed_remove"])
        # The `note` index is taken out of the config in the same change, so
        # the rebuild must only recreate the index on the surviving column.
        run_dbt(["run", "--models", "inc_indexed_remove", "--vars", "{drop_note: true}"])

        relation = relation_from_name(project.adapter, "inc_indexed_remove")
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "INTEGER"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, 10), (2, 20)]

        # The index on the surviving column is recreated; the index on the
        # removed column cannot be and is dropped with it.
        indexes = self.get_indexes("inc_indexed_remove", project, unique_schema)
        assert indexes == [{"columns": "id", "unique": False}]

    def test_remove_column_with_unique_index_removed_from_config(self, project, unique_schema):
        run_dbt(["run", "--models", "inc_indexed_remove_unique"])
        run_dbt(
            [
                "run",
                "--models",
                "inc_indexed_remove_unique",
                "--vars",
                "{drop_note: true}",
            ]
        )

        relation = relation_from_name(project.adapter, "inc_indexed_remove_unique")
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "INTEGER"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, 10), (2, 20)]

        # The unique index on the removed column was taken out of the config
        # in the same change, so the config-driven rebuild must not bring it
        # back; the surviving unique index comes back.
        indexes = self.get_indexes("inc_indexed_remove_unique", project, unique_schema)
        assert indexes == [{"columns": "id", "unique": True}]

    def test_add_only_sync_leaves_indexes_alone(self, project, unique_schema):
        run_dbt(["run", "--models", "inc_indexed_add"])

        relation = relation_from_name(project.adapter, "inc_indexed_add")
        # An index outside the model's config: an add-only sync must not
        # touch the indexes at all, so this one has to survive the run.
        project.run_sql(f"create index manual_amount_idx on {relation} (amount)")

        run_dbt(["run", "--models", "inc_indexed_add"])

        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "INTEGER"),
            ("note", "VARCHAR"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        # The added column is NULL for the pre-existing row.
        assert rows == [(1, 10, None), (2, 20, "y")]

        indexes = self.get_indexes("inc_indexed_add", project, unique_schema)
        assert sorted(indexes, key=lambda idx: idx["columns"]) == [
            {"columns": "amount", "unique": False},
            {"columns": "id", "unique": False},
        ]

    def test_failing_run_after_sync_restores_indexes(self, project, unique_schema):
        run_dbt(["run", "--models", "inc_indexed_failing_sync"])
        run_dbt(
            [
                "run",
                "--models",
                "inc_indexed_failing_sync",
                "--vars",
                "{drop_note: true, fail_after_sync: true}",
            ],
            expect_pass=False,
        )

        relation = relation_from_name(project.adapter, "inc_indexed_failing_sync")
        # The failing run committed the index drop (the duckdb/duckdb#24086
        # workaround), but the ALTER and the index rebuild run through
        # run_query, which executes with auto_begin=False, so they committed
        # immediately instead of joining a new transaction: the table is
        # left with the synced schema and its indexes, and only the failed
        # run's new rows are missing (the merge rolled back).
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "INTEGER"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, 10)]
        assert self.get_indexes("inc_indexed_failing_sync", project, unique_schema) == [
            {"columns": "id", "unique": False}
        ]

        # The next run re-runs the sync and brings the indexes back from the
        # model's `indexes` config.
        run_dbt(["run", "--models", "inc_indexed_failing_sync", "--vars", "{drop_note: true}"])
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "INTEGER"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, 10), (2, 20)]
        indexes = self.get_indexes("inc_indexed_failing_sync", project, unique_schema)
        assert indexes == [{"columns": "id", "unique": False}]

    def test_failing_run_after_type_change_restores_indexes(self, project, unique_schema):
        run_dbt(["run", "--models", "inc_indexed_failing_type_change"])
        run_dbt(
            [
                "run",
                "--models",
                "inc_indexed_failing_type_change",
                "--vars",
                "{fail_after_sync: true}",
            ],
            expect_pass=False,
        )

        relation = relation_from_name(project.adapter, "inc_indexed_failing_type_change")
        # Unlike a column removal (which runs through run_query and commits
        # immediately), a type change runs in a statement block, so the
        # ALTER and the index rebuild roll back with the failed transaction:
        # the table is left with its old types and no indexes.
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "INTEGER"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, 10)]
        assert self.get_indexes("inc_indexed_failing_type_change", project, unique_schema) == []

        # The next run re-runs the sync and brings everything back.
        run_dbt(["run", "--models", "inc_indexed_failing_type_change"])
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "VARCHAR"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, "10"), (2, "twenty")]
        indexes = self.get_indexes("inc_indexed_failing_type_change", project, unique_schema)
        assert sorted(indexes, key=lambda idx: idx["columns"]) == [
            {"columns": "amount", "unique": False},
            {"columns": "id", "unique": False},
        ]

    def test_type_change_keeps_indexes(self, project, unique_schema):
        run_dbt(["run", "--models", "inc_indexed_type_change"])
        run_dbt(["run", "--models", "inc_indexed_type_change"])

        relation = relation_from_name(project.adapter, "inc_indexed_type_change")
        columns = project.run_sql(f"describe {relation}", fetch="all")
        assert [(row[0], row[1]) for row in columns] == [
            ("id", "INTEGER"),
            ("amount", "VARCHAR"),
        ]
        rows = project.run_sql(f"select * from {relation} order by id", fetch="all")
        assert rows == [(1, "10"), (2, "twenty")]

        indexes = self.get_indexes("inc_indexed_type_change", project, unique_schema)
        assert sorted(indexes, key=lambda idx: idx["columns"]) == [
            {"columns": "amount", "unique": False},
            {"columns": "id", "unique": False},
        ]

    def get_indexes(self, table_name, project, unique_schema):
        sql = f"""
            SELECT
              sql as index_definition, is_unique
            FROM duckdb_indexes()
            WHERE
              schema_name = '{unique_schema}'
              AND
              table_name = '{table_name}'
        """
        results = project.run_sql(sql, fetch="all")
        return [self.parse_index_definition(row[0], row[1]) for row in results]

    def parse_index_definition(self, index_definition, is_unique):
        index_definition = index_definition.lower()
        m = INDEX_DEFINITION_PATTERN.search(index_definition)
        return {
            "columns": m.group(1),
            "unique": is_unique,
        }
