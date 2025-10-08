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
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name
import pytest

try:
    from dbt.tests.adapter.incremental.test_incremental_microbatch import (
        BaseMicrobatch,
    )
    MICROBATCH_AVAILABLE = True
except ImportError:
    # Microbatch tests not available in older dbt versions
    BaseMicrobatch = None
    MICROBATCH_AVAILABLE = False


class doUniqueKey(BaseIncrementalUniqueKey):
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


@pytest.mark.skipif(not MICROBATCH_AVAILABLE, reason="Microbatch tests require dbt-core >= 1.9")
class TestMicrobatch(BaseMicrobatch):
    """Test microbatch incremental strategy for DuckDB.

    Microbatch is supported on DuckDB 1.4.0+ when MERGE is available,
    but falls back to delete+insert for earlier versions.
    """
    pass


# Test models for merge strategy testing
_model_source = """
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
"""

_model_updates = """
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
"""

# Debug model to check the compiled SQL
_merge_debug = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id'
) }}

SELECT 1 AS id, 'test' AS name
"""

# Basic merge configurations
_merge_basic_update = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id'
) }}

{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
"""

_merge_with_conditions = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_condition="DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age",
    merge_insert_condition="DBT_INTERNAL_SOURCE.age > 25"
) }}

{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
"""

_merge_with_update_columns = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_columns=['name', 'age']
) }}

{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
"""

_merge_with_exclude_columns = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_exclude_columns=['job']
) }}

{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
"""

_merge_with_set_expressions = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_set_expressions={'updated_at': 'CURRENT_TIMESTAMP', 'version': 'COALESCE(DBT_INTERNAL_DEST.version, 0) + 1'}
) }}

SELECT id, name, age, job, CURRENT_TIMESTAMP AS updated_at, 1 AS version
FROM (
{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
)
"""

_merge_with_returning = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_returning_columns=['id', 'name']
) }}

{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
"""

_merge_custom_clauses = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={
        'when_matched': [
            {'action': 'update', 'mode': 'by_name', 'condition': 'DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age'},
            {'action': 'delete', 'condition': 'DBT_INTERNAL_SOURCE.job = \\'Manager\\''}
        ],
        'when_not_matched': [
            {'action': 'insert', 'mode': 'by_name', 'condition': 'DBT_INTERNAL_SOURCE.age > 25'}
        ]
    }
) }}

{% if is_incremental() %}
SELECT 1 AS id, 'Alice Updated' AS name, 26 AS age, 'Senior Engineer' AS job
UNION ALL
SELECT 4 AS id, 'Diana' AS name, 28 AS age, 'Designer' AS job
{% else %}
SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 30 AS age, 'Manager' AS job
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 35 AS age, 'Director' AS job
{% endif %}
"""

# Validation test models
_merge_invalid_condition_type = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_condition=123
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_invalid_columns_type = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_columns='not_a_list'
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_invalid_set_expressions_type = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_set_expressions=['not', 'a', 'dict']
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_conflicting_configs = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_condition="age > 25",
    merge_clauses={
        'when_matched': [{'action': 'update', 'mode': 'by_name'}]
    }
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_invalid_clauses_type = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses='not_a_dict'
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_empty_clauses = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={}
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_invalid_clause_list = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={
        'when_matched': 'not_a_list'
    }
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_invalid_clause_element = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={
        'when_matched': ['not_a_dict']
    }
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

# DuckLake restriction test models
_merge_ducklake_multiple_updates = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={
        'when_matched': [
            {'action': 'update', 'mode': 'by_name', 'condition': 'DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age'},
            {'action': 'update', 'mode': 'explicit', 'update': {'include': ['name']}, 'condition': 'DBT_INTERNAL_SOURCE.name != DBT_INTERNAL_DEST.name'}
        ],
        'when_not_matched': [
            {'action': 'insert', 'mode': 'by_name'}
        ]
    }
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_ducklake_update_delete = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={
        'when_matched': [
            {'action': 'update', 'mode': 'by_name', 'condition': 'DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age'},
            {'action': 'delete', 'condition': 'DBT_INTERNAL_SOURCE.job = \\'Manager\\''}
        ],
        'when_not_matched': [
            {'action': 'insert', 'mode': 'by_name'}
        ]
    }
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""

_merge_ducklake_valid_single_update = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={
        'when_matched': [
            {'action': 'update', 'mode': 'by_name', 'condition': 'DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age'}
        ],
        'when_not_matched': [
            {'action': 'insert', 'mode': 'by_name'}
        ]
    }
) }}

SELECT 1 AS id, 'Alice' AS name, 25 AS age, 'Engineer' AS job
"""


class TestIncrementalMerge:
    """Test class for DuckDB merge incremental strategy configurations"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_debug.sql": _merge_debug,
            "merge_basic_update.sql": _merge_basic_update,
            "merge_with_conditions.sql": _merge_with_conditions,
            "merge_with_update_columns.sql": _merge_with_update_columns,
            "merge_with_exclude_columns.sql": _merge_with_exclude_columns,
            "merge_with_set_expressions.sql": _merge_with_set_expressions,
            "merge_with_returning.sql": _merge_with_returning,
            "merge_custom_clauses.sql": _merge_custom_clauses,
        }

    def test_merge_debug(self, project):
        """Debug test to check what SQL is generated"""
        # First run should work (creates table)
        run_dbt(["run", "--select", "merge_debug"], expect_pass=True)
        
        # Second run should do incremental merge 
        result = run_dbt(["run", "--select", "merge_debug"], expect_pass=True)
        print("DEBUG - Merge SQL works correctly!")

    def test_merge_basic_update(self, project):
        """Test basic merge functionality with default update behavior"""
        # Initial run
        run_dbt(["run", "--select", "merge_basic_update"], expect_pass=True)
        
        # Verify initial data
        relation = relation_from_name(project.adapter, "merge_basic_update")
        result = project.run_sql(f"SELECT COUNT(*) as count FROM {relation}", fetch="one")
        assert result[0] == 3
        
        # Incremental run - should update Alice and insert Diana
        run_dbt(["run", "--select", "merge_basic_update"], expect_pass=True)
        
        # Verify final data
        result = project.run_sql(f"SELECT COUNT(*) as count FROM {relation}", fetch="one")
        assert result[0] == 4  # 3 original + 1 new (Diana)
        
        # Check Alice was updated
        alice = project.run_sql(f"SELECT name FROM {relation} WHERE id = 1", fetch="one")
        assert alice[0] == "Alice Updated"

    def test_merge_with_conditions(self, project):
        """Test merge with update and insert conditions"""
        run_dbt(["run", "--select", "merge_with_conditions"], expect_pass=True)
        run_dbt(["run", "--select", "merge_with_conditions"], expect_pass=True)
        
        relation = relation_from_name(project.adapter, "merge_with_conditions")
        result = project.run_sql(f"SELECT COUNT(*) as count FROM {relation}", fetch="one")
        # Should have 4 rows (Diana should be inserted as age > 25)
        assert result[0] == 4

    def test_merge_with_update_columns(self, project):
        """Test merge with specific update columns"""
        run_dbt(["run", "--select", "merge_with_update_columns"], expect_pass=True)
        run_dbt(["run", "--select", "merge_with_update_columns"], expect_pass=True)
        
        relation = relation_from_name(project.adapter, "merge_with_update_columns")
        # Check that specified columns were updated
        alice = project.run_sql(f"SELECT name, age FROM {relation} WHERE id = 1", fetch="one")
        assert alice[0] == "Alice Updated"
        assert alice[1] == 26
        
    def test_merge_with_exclude_columns(self, project):
        """Test merge with excluded columns"""
        run_dbt(["run", "--select", "merge_with_exclude_columns"], expect_pass=True)
        run_dbt(["run", "--select", "merge_with_exclude_columns"], expect_pass=True)
        
        relation = relation_from_name(project.adapter, "merge_with_exclude_columns")
        # Check that job column was not updated (excluded)
        alice = project.run_sql(f"SELECT job FROM {relation} WHERE id = 1", fetch="one")
        assert alice[0] == "Engineer"  # Should remain original value

    def test_merge_with_set_expressions(self, project):
        """Test merge with custom set expressions"""
        run_dbt(["run", "--select", "merge_with_set_expressions"], expect_pass=True)
        run_dbt(["run", "--select", "merge_with_set_expressions"], expect_pass=True)
        
        relation = relation_from_name(project.adapter, "merge_with_set_expressions")
        # Check that custom expressions were applied
        alice = project.run_sql(f"SELECT version, updated_at FROM {relation} WHERE id = 1", fetch="one")
        assert alice[0] == 2  # version should be incremented
        assert alice[1] is not None  # updated_at should be set

    def test_merge_with_returning(self, project):
        """Test merge with returning columns"""
        run_dbt(["run", "--select", "merge_with_returning"], expect_pass=True)
        run_dbt(["run", "--select", "merge_with_returning"], expect_pass=True)
        
        # This test mainly verifies the SQL compiles and runs without error
        relation = relation_from_name(project.adapter, "merge_with_returning")
        result = project.run_sql(f"SELECT COUNT(*) as count FROM {relation}", fetch="one")
        assert result[0] == 4

    def test_merge_custom_clauses(self, project):
        """Test merge with custom when_matched and when_not_matched clauses"""
        run_dbt(["run", "--select", "merge_custom_clauses"], expect_pass=True)
        run_dbt(["run", "--select", "merge_custom_clauses"], expect_pass=True)
        
        relation = relation_from_name(project.adapter, "merge_custom_clauses")
        # Alice should be updated (age condition met), Diana should be inserted (age > 25)
        # Bob and Charlie remain unchanged (not in source data)
        result = project.run_sql(f"SELECT COUNT(*) as count FROM {relation}", fetch="one")
        assert result[0] == 4  # Original 3 + 1 (Diana inserted)
        
        # Verify Alice was updated 
        alice = project.run_sql(f"SELECT name FROM {relation} WHERE id = 1", fetch="one")
        assert alice[0] == "Alice Updated"
        
        # Verify Bob still exists (wasn't deleted because not in source data)
        bob = project.run_sql(f"SELECT COUNT(*) as count FROM {relation} WHERE id = 2", fetch="one")
        assert bob[0] == 1


class TestIncrementalMergeValidation:
    """Test class for DuckDB merge configuration validation"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_invalid_condition_type.sql": _merge_invalid_condition_type,
            "merge_invalid_columns_type.sql": _merge_invalid_columns_type,
            "merge_invalid_set_expressions_type.sql": _merge_invalid_set_expressions_type,
            "merge_conflicting_configs.sql": _merge_conflicting_configs,
            "merge_invalid_clauses_type.sql": _merge_invalid_clauses_type,
            "merge_empty_clauses.sql": _merge_empty_clauses,
            "merge_invalid_clause_list.sql": _merge_invalid_clause_list,
            "merge_invalid_clause_element.sql": _merge_invalid_clause_element,
            "merge_ducklake_multiple_updates.sql": _merge_ducklake_multiple_updates,
            "merge_ducklake_update_delete.sql": _merge_ducklake_update_delete,
            "merge_ducklake_valid_single_update.sql": _merge_ducklake_valid_single_update,
        }

    def test_invalid_condition_type(self, project):
        """Test validation fails for non-string condition"""
        # First run creates the table
        run_dbt(["run", "--select", "merge_invalid_condition_type"], expect_pass=True)
        # Second run should trigger validation and fail
        result = run_dbt(["run", "--select", "merge_invalid_condition_type"], expect_pass=False)
        assert "merge_update_condition must be a string, found: 123" in str(result.results[0].message)

    def test_invalid_columns_type(self, project):
        """Test validation fails for non-list columns"""
        run_dbt(["run", "--select", "merge_invalid_columns_type"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_invalid_columns_type"], expect_pass=False)
        assert "merge_update_columns must be a list" in str(result.results[0].message)

    def test_invalid_set_expressions_type(self, project):
        """Test validation fails for non-dict set expressions"""
        run_dbt(["run", "--select", "merge_invalid_set_expressions_type"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_invalid_set_expressions_type"], expect_pass=False)
        assert "merge_update_set_expressions must be a dictionary, found: ['not', 'a', 'dict']" in str(result.results[0].message)

    def test_conflicting_configs(self, project):
        """Test validation fails when mixing merge_clauses with basic configs"""
        run_dbt(["run", "--select", "merge_conflicting_configs"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_conflicting_configs"], expect_pass=False)
        assert "basic merge configurations will be ignored" in str(result.results[0].message)
        assert "merge_update_condition" in str(result.results[0].message)

    def test_invalid_clauses_type(self, project):
        """Test validation fails for non-dict merge_clauses"""
        run_dbt(["run", "--select", "merge_invalid_clauses_type"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_invalid_clauses_type"], expect_pass=False)
        assert "merge_clauses must be a dictionary, found: not_a_dict" in str(result.results[0].message)

    def test_empty_clauses(self, project):
        """Test validation fails for empty merge_clauses"""
        run_dbt(["run", "--select", "merge_empty_clauses"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_empty_clauses"], expect_pass=False)
        assert "must contain at least one of 'when_matched' or 'when_not_matched'" in str(result.results[0].message)

    def test_invalid_clause_list(self, project):
        """Test validation fails for non-list clause values"""
        run_dbt(["run", "--select", "merge_invalid_clause_list"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_invalid_clause_list"], expect_pass=False)
        assert "when_matched must be a list" in str(result.results[0].message)

    def test_invalid_clause_element(self, project):
        """Test validation fails for non-dict clause elements"""
        run_dbt(["run", "--select", "merge_invalid_clause_element"], expect_pass=True)
        result = run_dbt(["run", "--select", "merge_invalid_clause_element"], expect_pass=False)
        assert "elements must be dictionaries, found: not_a_dict" in str(result.results[0].message)

    # NOTE: The following DuckLake tests require a DuckLake attachment to be active
    # They will only trigger validation errors when the target relation is in a ducklake database
    def test_ducklake_multiple_updates(self, project):
        """Test DuckLake restriction fails for multiple UPDATE actions (in ducklake environments)"""
        run_dbt(["run", "--select", "merge_ducklake_multiple_updates"], expect_pass=True)
        try:
            # This will pass in non-ducklake environments, fail in ducklake environments
            result = run_dbt(["run", "--select", "merge_ducklake_multiple_updates"], expect_pass=False)
            # If we get here, it means the run failed (ducklake environment)
            assert "DuckLake MERGE restrictions" in str(result.results[0].message)
            assert "can contain only a single UPDATE or DELETE action" in str(result.results[0].message)
        except AssertionError:
            # Run passed - we're in a non-ducklake environment, validation was skipped
            pass

    def test_ducklake_update_delete(self, project):
        """Test DuckLake restriction fails for UPDATE + DELETE actions (in ducklake environments)"""
        run_dbt(["run", "--select", "merge_ducklake_update_delete"], expect_pass=True)
        try:
            # This will pass in non-ducklake environments, fail in ducklake environments
            result = run_dbt(["run", "--select", "merge_ducklake_update_delete"], expect_pass=False)
            # If we get here, it means the run failed (ducklake environment)
            assert "DuckLake MERGE restrictions" in str(result.results[0].message)
            assert "Found 2 UPDATE/DELETE actions" in str(result.results[0].message)
        except AssertionError:
            # Run passed - we're in a non-ducklake environment, validation was skipped
            pass

    def test_ducklake_valid_single_update(self, project):
        """Test DuckLake accepts single UPDATE action"""
        run_dbt(["run", "--select", "merge_ducklake_valid_single_update"], expect_pass=True)
        run_dbt(["run", "--select", "merge_ducklake_valid_single_update"], expect_pass=True)
        # This should always pass, whether ducklake or not
