import pytest

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from dbt.tests.util import run_dbt

@pytest.mark.skip_profile("md")
class TestPersistDocs(BasePersistDocs):
    pass


@pytest.mark.skip_profile("md")
class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


@pytest.mark.skip_profile("md")
class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass

@pytest.mark.requires_ducklake
@pytest.mark.skip_profile("md", "buenavista")
class TestDuckLakePersistDocsTransactions:
    @pytest.fixture(scope="class")
    def ducklake_paths(self, tmp_path_factory):
        base = tmp_path_factory.mktemp("ducklake-persist-docs")
        data_path = base / "data"
        data_path.mkdir()
        return {
            "metadata_path": str(base / "metadata.ducklake"),
            "data_path": str(data_path),
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, ducklake_paths):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "extensions": ["ducklake"],
                        "attach": [
                            {
                                "path": f"ducklake:{ducklake_paths['metadata_path']}",
                                "alias": "ducklake_docs",
                                "options": {
                                    "data_path": ducklake_paths["data_path"],
                                },
                            }
                        ],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                }
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": """
version: 2
models:
  - name: ducklake_broken_docs
    description: "DuckLake broken docs comment"
    columns:
      - name: id
        description: "DuckLake id comment"
""",
            "ducklake_broken_docs.sql": """
{{ config(materialized='table', database='ducklake_docs') }}
select 1 as id
""",
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "ducklake_alter_relation_comment.sql": """
{% macro duckdb__alter_relation_comment(relation, comment) %}
  {% if var('fail_relation_comment', false) %}
    select * from missing_relation_for_persist_docs_failure
  {% else %}
    {% set escaped_comment = duckdb_escape_comment(comment) %}
    comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
  {% endif %}
{% endmacro %}
""",
        }

    def test_ducklake_post_commit_docs_failure_keeps_table_and_recovers(self, project):
        run_dbt(
            [
                "run",
                "--select",
                "ducklake_broken_docs",
                "--vars",
                '{"fail_relation_comment": true}',
            ],
            expect_pass=False,
        )

        row_count = project.run_sql(
            "select count(*) from ducklake_docs.main.ducklake_broken_docs",
            fetch="one",
        )[0]
        assert row_count == 1

        table_comment = project.run_sql(
            """
            select table_comment
            from information_schema.tables
            where table_catalog = 'ducklake_docs'
              and table_schema = 'main'
              and table_name = 'ducklake_broken_docs'
            """,
            fetch="one",
        )[0]
        assert table_comment is None

        rerun_results = run_dbt(["run", "--select", "ducklake_broken_docs"])
        assert len(rerun_results) == 1

        recovered_comment = project.run_sql(
            """
            select table_comment
            from information_schema.tables
            where table_catalog = 'ducklake_docs'
              and table_schema = 'main'
              and table_name = 'ducklake_broken_docs'
            """,
            fetch="one",
        )[0]
        assert recovered_comment == "DuckLake broken docs comment"
