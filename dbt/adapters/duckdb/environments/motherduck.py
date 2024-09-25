from typing import Optional

from .. import credentials
from .local import DuckDBConnectionWrapper
from .local import LocalEnvironment
from dbt.adapters.contracts.connection import AdapterResponse


MOTHERDUCK_SAAS_MODE_QUERY = """
SELECT value FROM duckdb_settings() WHERE name = 'motherduck_saas_mode'
"""


class MotherDuckEnvironment(LocalEnvironment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        self._motherduck_saas_mode: Optional[bool] = None
        super().__init__(credentials)

    def motherduck_saas_mode(self, handle: DuckDBConnectionWrapper):
        # Return cached value
        if self._motherduck_saas_mode is True:
            return True
        # Get SaaS mode from DuckDB config
        con = handle.cursor()
        (motherduck_saas_mode,) = con.sql(MOTHERDUCK_SAAS_MODE_QUERY).fetchone()
        if str(motherduck_saas_mode).lower() in ["1", "true"]:
            self._motherduck_saas_mode = True
            return True
        return False

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        # Block local file access if SaaS mode is on
        if self.motherduck_saas_mode(handle) is True:
            raise RuntimeError("Python models are disabled when MotherDuck SaaS Mode is on.")
        return super().submit_python_job(
            handle=handle, parsed_model=parsed_model, compiled_code=compiled_code
        )
