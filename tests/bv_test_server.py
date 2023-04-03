import importlib.util
import os
import tempfile

from buenavista.backends.duckdb import DuckDBConnection
from buenavista.core import BVType, Extension, Session, QueryResult, SimpleQueryResult
from buenavista.postgres import BuenaVistaServer

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment


class TestPythonRunner(Extension):
    def type(self) -> str:
        return "dbt_python_job"

    def apply(self, params: dict, handle: Session) -> QueryResult:
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        mod_file.write(params["module_definition"].lstrip().encode("utf-8"))
        mod_file.close()
        try:
            spec = importlib.util.spec_from_file_location(
                params["module_name"],
                mod_file.name,
            )
            if not spec:
                raise Exception("Failed to load python model as module")
            module = importlib.util.module_from_spec(spec)
            if spec.loader:
                spec.loader.exec_module(module)
            else:
                raise Exception("Module spec did not include a loader")
            # Do the actual work to run the code here
            cursor = handle.cursor()
            dbt = module.dbtObj(handle.load_df_function)
            df = module.model(dbt, cursor)
            module.materialize(df, cursor)
            return SimpleQueryResult("msg", "Success", BVType.TEXT)
        finally:
            os.unlink(mod_file.name)


def create():
    config = {"path": ":memory:", "type": "duckdb"}
    creds = DuckDBCredentials.from_dict(config)
    db = Environment.initialize_db(creds)
    conn = DuckDBConnection(db)
    server = BuenaVistaServer(
        ("localhost", 5433), conn, extensions=[TestPythonRunner()]
    )
    return server


if __name__ == "__main__":
    server = create()
    server.serve_forever()
