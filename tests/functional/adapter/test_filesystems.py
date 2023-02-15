import os

import pytest
from dbt.tests.util import run_dbt

models_file_model_sql = """select * from read_csv_auto('file:///tmp/foo.csv')"""


class TestFilesystems:
    @pytest.fixture(scope="class")
    def profiles_config_update(self):

        return {
            "test": {
                "outputs": {
                    "dev": {"type": "duckdb", "path": ":memory:", "filesystems": [{"fs": "file"}]}
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_model.sql": models_file_model_sql,
        }

    @pytest.fixture(scope="class")
    def foo_file(self):
        with open("/tmp/foo.csv", "w") as f:
            f.write("id,a,b\n1,2,3\n4,5,6\n7,8,9")
        yield
        os.unlink("/tmp/foo.csv")

    def test_filesystems(self, foo_file, project):
        results = run_dbt()
        assert len(results) == 1
