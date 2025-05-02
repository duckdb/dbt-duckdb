from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup
)
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt
import pytest


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    def test__bad_unique_key_list(self, project):
        """expect compilation error from unique key not being a column"""

        (status, exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key_list"
        )

        assert status == RunStatus.Error
        # MotherDuck has a `dbt_temp` workaround for incremental runs which causes this test to fail
        # because the error message is being truncated with DuckDB >= 1.2.0
        if not project.adapter.config.credentials.is_motherduck:
            assert "thisisnotacolumn" in exc.lower()


class TestIncrementalPredicates(BaseIncrementalPredicates):
    pass


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):     
    pass
    

models__incremental_append_new_columns_with_space = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = dbt.type_string() %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2,
       cast(field3 as {{string_type}}) as "field 3",
       cast(field4 as {{string_type}}) as "field 4"
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2
FROM source_data where id <= 3

{% endif %}
"""

class TestIncrementalOnSchemaChangeQuotingFalse(BaseIncrementalOnSchemaChangeSetup):     
    """ We need a new class based on the _Setup base class to allow project config change without repeating all other tests"""
    @pytest.fixture(scope="class")
    def models(self):
        """ Override the models test fixture with the custom one injected """ 
        # Get the original models dict
        mods = dict(BaseIncrementalOnSchemaChange.models.__wrapped__(self))
        # Add the custom model
        mods["incremental_append_new_columns_with_space.sql"] = models__incremental_append_new_columns_with_space
        return mods
        
    
    def run_twice_and_return_status(self, select, expect_pass_2nd_run):
        """Two runs of the specified models - return the status and message from the second"""
        run_dbt(
            ["run", "--select", select, "--full-refresh"],
            expect_pass=True,
        )
        run_result = run_dbt(
            ["run", "--select", select], expect_pass=expect_pass_2nd_run
        ).results[-1]
        return run_result.status, run_result.message

        
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": False}}
        
    
    def test__handle_identifier_quoting_config_false(self, project):        
        # it should fail if quoting is set to false
        (status, exc) = self.run_twice_and_return_status(
            select="model_a incremental_append_new_columns_with_space",
            expect_pass_2nd_run=False
        )
        assert status == RunStatus.Error

class TestIncrementalOnSchemaChangeQuotingTrue(BaseIncrementalOnSchemaChangeSetup):     
    """ We need a new class based on the _Setup base class to allow project config change without repeating all other tests"""
    @pytest.fixture(scope="class")
    def models(self):
        """ Override the models test fixture with the custom one injected """ 
        # Get the original models dict
        mods = dict(BaseIncrementalOnSchemaChange.models.__wrapped__(self))
        # Add the custom model
        mods["incremental_append_new_columns_with_space.sql"] = models__incremental_append_new_columns_with_space
        return mods
        
    
    def run_twice_and_return_status(self, select, expect_pass_2nd_run):
        """Two runs of the specified models - return the status and message from the second"""
        run_dbt(
            ["run", "--select", select, "--full-refresh"],
            expect_pass=True,
        )
        run_result = run_dbt(
            ["run", "--select", select], expect_pass=expect_pass_2nd_run
        ).results[-1]
        return run_result.status, run_result.message

        
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": True}}
        
    
    def test__handle_identifier_quoting_config_false(self, project):        
        # it should fail if quoting is set to false
        (status, exc) = self.run_twice_and_return_status(
            select="model_a incremental_append_new_columns_with_space",
            expect_pass_2nd_run=True
        )
        assert status == RunStatus.Success
    
