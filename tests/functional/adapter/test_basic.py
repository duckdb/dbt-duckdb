import duckdb
import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_incremental import BaseIncrementalNotSchemaChange
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
)
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    no_stats,
    expected_references_catalog,
)
from dbt.tests.util import run_dbt


catalog_relations_alpha_model_sql = """
{{ config(materialized='table') }}

select 1 as id, 'main' as note
"""

catalog_relations_beta_model_sql = """
{{ config(materialized='table') }}

select 2 as id, 3.14 as score
"""

catalog_relations_macros_sql = """
{% macro catalog_row_keys(result) %}
    {% set ns = namespace(keys=[]) %}
    {% set table_database = result.columns['table_database'] %}
    {% set table_schema = result.columns['table_schema'] %}
    {% set table_name = result.columns['table_name'] %}
    {% set column_name = result.columns['column_name'] %}

    {% for i in range(result.rows | length) %}
        {% do ns.keys.append(
            table_database[i] ~ '.' ~ table_schema[i] ~ '.' ~ table_name[i] ~ '.' ~ column_name[i]
        ) %}
    {% endfor %}

    {{ return(ns.keys) }}
{% endmacro %}


{% macro assert_get_catalog_relations() %}
    {% set relation_query %}
        select database_name, schema_name
        from duckdb_columns()
        where upper(table_name) = upper('beta_model')
          and upper(column_name) = upper('score')
    {% endset %}
    {% set relation_result = run_query(relation_query) %}
    {% set database = relation_result.columns['database_name'][0] %}
    {% set schema = relation_result.columns['schema_name'][0] %}
    {% set info_schema = api.Relation.create(database=database, schema='information_schema') %}
    {% set specific_results = get_catalog_relations(
        info_schema,
        [api.Relation.create(database=database, schema=schema | upper, identifier='ALPHA_MODEL')]
    ) %}
    {% set schema_results = get_catalog_relations(
        info_schema,
        [api.Relation.create(database=database, schema=schema | upper)]
    ) %}

    {% set specific_keys = catalog_row_keys(specific_results) %}
    {% set schema_keys = catalog_row_keys(schema_results) %}
    {% set expected_specific = [
        database ~ '.' ~ schema ~ '.alpha_model.id',
        database ~ '.' ~ schema ~ '.alpha_model.note',
    ] %}
    {% set expected_schema = expected_specific + [
        database ~ '.' ~ schema ~ '.beta_model.id',
        database ~ '.' ~ schema ~ '.beta_model.score',
    ] %}

    {% if specific_keys != expected_specific %}
        {% do exceptions.raise_compiler_error(
            'Unexpected catalog rows for specific relation: ' ~ specific_keys ~ ' expected ' ~ expected_specific
        ) %}
    {% endif %}

    {% if schema_keys != expected_schema %}
        {% do exceptions.raise_compiler_error(
            'Unexpected catalog rows for schema relation: ' ~ schema_keys ~ ' expected ' ~ expected_schema
        ) %}
    {% endif %}
{% endmacro %}
"""


class TestSimpleMaterializationsDuckDB(BaseSimpleMaterializations):
    pass


class TestSingularTestsDuckDB(BaseSingularTests):
    pass


class TestSingularTestsEphemeralDuckDB(BaseSingularTestsEphemeral):
    pass


class TestEmptyDuckDB(BaseEmpty):
    pass


class TestEphemeralDuckDB(BaseEphemeral):
    pass


class TestIncrementalDuckDB(BaseIncremental):
    pass

class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


class TestGenericTestsDuckDB(BaseGenericTests):
    pass


class TestSnapshotCheckColsDuckDB(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampDuckDB(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodDuckDB(BaseAdapterMethod):
    pass


class TestValidateConnectionDuckDB(BaseValidateConnection):
    pass


class TestDocsGenerateDuckDB(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=None,
            id_type="INTEGER",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )


class TestDocsGenReferencesDuckDB(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role=None,
            id_type="INTEGER",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
            bigint_type="BIGINT",
        )


@pytest.mark.skip_profile("buenavista", "md")
class TestCatalogRelationsDuckDB:
    @pytest.fixture(scope="class")
    def attach_test_db(self, tmp_path_factory, unique_schema):
        path = str(tmp_path_factory.mktemp("catalog-relations") / "attach_test.duckdb")
        db = duckdb.connect(path)
        try:
            db.execute(f'create schema "{unique_schema}"')
            db.execute(
                f"""
                create table "{unique_schema}"."alpha_model" as
                select 999 as shadow_id, 'attached' as shadow_note, true as shadow_flag
                """
            )
        finally:
            db.close()
        return path

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, attach_test_db, unique_schema):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "schema": unique_schema,
                        "attach": [{"path": attach_test_db}],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "alpha_model.sql": catalog_relations_alpha_model_sql,
            "beta_model.sql": catalog_relations_beta_model_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "catalog_relations.sql": catalog_relations_macros_sql,
        }

    def test_get_catalog_relations(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        run_dbt(
            [
                "run-operation",
                "assert_get_catalog_relations",
            ]
        )
