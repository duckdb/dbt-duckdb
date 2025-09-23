import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
    basic_sql,
    m_1,
    schema_yml,
    second_sql,
)
from dbt.tests.util import run_dbt

basic_python_template = """
import pandas as pd

def model(dbt, _):
    dbt.config(
        materialized='table',
    )
    pdf = pd.DataFrame()
    df =  dbt.ref("my_sql_model")
    df2 = dbt.source('test_source', 'test_table')
    df = df.limit(2)
    return df{extension}
"""

# incremental_python_template =  """
# import pandas as pd

# def model(dbt, session):
#     dbt.config(materialized="incremental")

#     df = pd.DataFrame({{
#         'id': [1, 2, 5],
#         'value': ['A', 'B', 'C']
#     }})

#     if dbt.is_incremental:
#         existing_query = f"SELECT id FROM {{dbt.this}}"
#         existing_ids = session.sql(existing_query).df()
        
#         if not existing_ids.empty:
#             df = df[~df['id'].isin(existing_ids['id'])]

#     return df{extension}
# """

class TestBasePythonModelDuckDBPyRelation(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_versioned_sql_model_v1.sql": basic_sql,
            "my_python_model.py": basic_python_template.format(extension=""),
            "second_sql_model.sql": second_sql,
        }


class TestBasePythonModelPandasDF(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_versioned_sql_model_v1.sql": basic_sql,
            "my_python_model.py": basic_python_template.format(extension=".df()"),
            "second_sql_model.sql": second_sql,
        }


incremental_python_template = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key='id')
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        # incremental runs should only apply to part of the data
        df = df.filter("id > 5")
    return df{extension}
"""

# TODO(jwills): figure out why this one doesn't work; I think it's a test utils issue
@pytest.mark.skip_profile("buenavista")
class TestBasePythonIncremental(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "delete+insert"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": m_1, 
                # model returns a Pandas dataframe
                "incremental_pandas.py": incremental_python_template.format(extension=".df()"),
                # model returns a DuckDBPyRelation, which is handled slightly differently, see Environment.run_python_job
                "incremental.py": incremental_python_template.format(extension=""),  }  


    @pytest.mark.parametrize("model_to_test", ["incremental", "incremental_pandas"])
    def test_incremental(self, project, model_to_test):
        # create m_1 and run incremental model the first time
        run_dbt(["run"])
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.{model_to_test}",
                fetch="one",
            )[0]
            == 5
        )
        # running incremental model again will not cause any changes in the result model
        run_dbt(["run", "-s", model_to_test])
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.{model_to_test}",
                fetch="one",
            )[0]
            == 5
        )

        # add 3 records with one supposed to be filtered out
        project.run_sql(f"insert into {test_schema_relation}.m_1(id) values (0), (6), (7)")

        run_dbt(["run", "-s", model_to_test])
        # validate that incremental model would correctly add 2 valid records to result model
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.{model_to_test}",
                fetch="one",
            )[0]
            == 7
        )


empty_upstream_model_python = """
def model(dbt, con):
    dbt.config(
        materialized='table',
    )
    return con.query("select 'a'::varchar as a, 0::boolean as b limit 0")
"""


class TestEmptyPythonModel:
    """
    This test ensures that Python models returning a DuckDBPyRelation are materialized
    with the correct schema, even when empty. I.e. ensure pyarrow is being used instead
    of pandas.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.py": empty_upstream_model_python,
        }

    def test_run(self, project):
        run_dbt(["run"])
        result = project.run_sql(
            """
            select column_name, data_type from system.information_schema.columns
            where table_name='upstream_model' order by column_name
            """,
            fetch="all",
        )
        assert result == [("a", "VARCHAR"), ("b", "BOOLEAN")]

temp_upstream_model_python = """
def model(dbt, con):
    dbt.config(
        materialized='table',
    )
    con.execute("create temp table t(a int)")
    return con.table("t")
"""


class TestTempTablePythonModel:
    """
    This test ensures that Python models returning a DuckDBPyRelation based
    on a temporary duckdb table can still be materialized
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.py": temp_upstream_model_python,
        }
    
    def test_run(self, project):
        run_dbt(["run"])

python_pyarrow_table_model = """
import pyarrow as pa

def model(dbt, con):
    return pa.Table.from_pydict({"a": [1,2,3]})
"""

python_pyarrow_dataset_model = """
import pyarrow as pa
import pyarrow.dataset as ds

def model(dbt, con):
    return ds.dataset(pa.Table.from_pydict({"b": [4, 5, 6]}))
"""


class TestMultiThreadedImports:
    """
    This test ensures that multiple pyarrow models can run concurrently with threads > 1
    and not suffer import issues.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_table1.py": python_pyarrow_table_model,
            "model_table2.py": python_pyarrow_table_model,
            "model_table3.py": python_pyarrow_table_model,
            "model_dataset1.py": python_pyarrow_dataset_model,
            "model_dataset2.py": python_pyarrow_dataset_model,
            "model_dataset3.py": python_pyarrow_dataset_model,
        }

    def test_run(self, project):
        run_dbt(["run"])
