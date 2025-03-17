import pytest
import re
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
)
from tests.functional.adapter.indexes.fixtures import (
    models__incremental_sql,
    models__table_sql,
    seeds__seed_csv,
    snapshots__colors_sql,
)


INDEX_DEFINITION_PATTERN = re.compile(r"\((.*?)\)")


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

    def assertCountEqual(self, a, b):
        assert len(a) == len(b)
