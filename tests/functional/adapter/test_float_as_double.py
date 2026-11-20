import pytest

from dbt.tests.util import run_dbt

prices_csv = """
id,price,ratio,small
1,19.99,0.1,0.5
2,0.01,0.2,1.5
""".lstrip()

seeds_yml = """
seeds:
  - name: prices
    config:
      column_types:
        price: float
        ratio: ' Float '
        small: float4
  - name: prices_slow
    config:
      fast: false
      column_types:
        price: float
"""

type_float_model_sql = """
select cast(19.99 as {{ dbt.type_float() }}) as price
"""


def column_types(project, table):
    rows = project.run_sql(
        f"""
        select column_name, data_type
        from information_schema.columns
        where table_schema = '{project.test_schema}' and table_name = '{table}'
        """,
        fetch="all",
    )
    return dict(rows)


class FloatAsDoubleBase:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "prices.csv": prices_csv,
            "prices_slow.csv": prices_csv,
            "seeds.yml": seeds_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"type_float_model.sql": type_float_model_sql}


class TestFloatAsDoubleDisabled(FloatAsDoubleBase):
    """Without the behavior flag, a bare `float` keeps DuckDB's 4-byte FLOAT."""

    def test_float_stays_single_precision(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        assert column_types(project, "prices") == {
            "id": "INTEGER",
            "price": "FLOAT",
            "ratio": "FLOAT",
            "small": "FLOAT",
        }
        assert column_types(project, "prices_slow")["price"] == "FLOAT"
        assert column_types(project, "type_float_model") == {"price": "FLOAT"}


class TestFloatAsDoubleEnabled(FloatAsDoubleBase):
    """With duckdb_float_as_double, a bare `float` means DOUBLE, as on Postgres."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"duckdb_float_as_double": True}}

    def test_float_becomes_double(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # float4 states its width, so it stays single precision
        assert column_types(project, "prices") == {
            "id": "INTEGER",
            "price": "DOUBLE",
            "ratio": "DOUBLE",
            "small": "FLOAT",
        }
        assert column_types(project, "prices_slow")["price"] == "DOUBLE"
        assert column_types(project, "type_float_model") == {"price": "DOUBLE"}

        # the values keep their precision
        for table in ("prices", "prices_slow", "type_float_model"):
            price = project.run_sql(
                f"select min(price) filter (where price > 1) from {project.test_schema}.{table}",
                fetch="one",
            )[0]
            assert price == 19.99, table
