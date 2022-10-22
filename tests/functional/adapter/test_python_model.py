import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
    basic_sql,
    m_1,
    schema_yml,
    second_sql,
)

basic_python = """
def model(dbt, _):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("my_sql_model")
    df2 = dbt.source('test_source', 'test_table')
    df = df.limit(2)
    return df.df()
"""


class TestBasePythonModel(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_python_model.py": basic_python,
            "second_sql_model.sql": second_sql,
        }


incremental_python = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key='id')
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        # incremental runs should only apply to part of the data
        df = df.filter("id > 5")
    return df.df()
"""


class TestBasePythonIncremental(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "delete+insert"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": m_1, "incremental.py": incremental_python}
