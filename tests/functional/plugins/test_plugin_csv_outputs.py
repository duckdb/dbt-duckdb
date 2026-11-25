import os
import sqlite3
import tempfile

import pytest

from dbt.tests.util import run_dbt

model_template = """
    {{{{ config(materialized='external', plugin='sql', format='csv'{options}) }}}}
    select 1 as id, 'a' as name union all select 2, 'b'
"""

csv_models = {
    "csv_default": "",
    "csv_delimiter": ", delimiter='|'",
    "csv_options_delimiter": ", options={'delimiter': ';'}",
    "csv_no_header": ", options={'header': false}",
    "csv_gzip": ", options={'compression': 'gzip'}",
}


@pytest.mark.skip_profile("buenavista", "md")
class TestPluginCsvOutputs:
    """Plugins that store an external CSV model read it back with its write options."""

    @pytest.fixture(scope="class")
    def sqlite_path(self):
        fd, path = tempfile.mkstemp(prefix="satest_csv_", suffix=".db")
        os.close(fd)
        yield path
        os.unlink(path)

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, sqlite_path):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": [
                            {
                                "module": "sqlalchemy",
                                "alias": "sql",
                                "config": {"connection_url": f"sqlite:///{sqlite_path}"},
                            }
                        ],
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{name}.sql": model_template.format(options=options)
            for name, options in csv_models.items()
        }

    def test_csv_outputs(self, project, sqlite_path):
        results = run_dbt()
        assert len(results) == len(csv_models)

        db = sqlite3.connect(sqlite_path)
        try:
            for name in csv_models:
                # the stored table matches the model's relation in DuckDB
                expected_columns = [
                    r[0]
                    for r in project.run_sql(
                        f"select column_name from information_schema.columns "
                        f"where table_name = '{name}' order by ordinal_position",
                        fetch="all",
                    )
                ]
                if name != "csv_no_header":
                    assert expected_columns == ["id", "name"], name
                columns = [r[1] for r in db.execute(f"PRAGMA table_info({name})")]
                assert columns == expected_columns, name
                rows = db.execute(f"SELECT * FROM {name} ORDER BY 1").fetchall()
                assert rows == [(1, "a"), (2, "b")], name
        finally:
            db.close()
