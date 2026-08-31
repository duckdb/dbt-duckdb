from typing import Any
from typing import Dict
from typing import Optional

from .. import credentials
from ..credentials import FlightConfig
from .flights import FlightRunner
from .local import DuckDBConnectionWrapper
from .local import LocalEnvironment
from dbt.adapters.contracts.connection import AdapterResponse


MOTHERDUCK_SAAS_MODE_QUERY = """
SELECT value FROM duckdb_settings() WHERE name = 'motherduck_saas_mode'
"""

LOCAL_SUBMISSION = "local"
FLIGHT_SUBMISSION = "flight"
SUBMISSION_METHODS = (LOCAL_SUBMISSION, FLIGHT_SUBMISSION)

SAAS_MODE_ERROR = (
    "Python models are disabled when MotherDuck SaaS Mode is on. Set "
    "`submission_method: flight` on the model (or `flights: {enabled_by_default: true}` "
    "in your profile) to run it on MotherDuck Flights instead."
)


class MotherDuckEnvironment(LocalEnvironment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        self._motherduck_saas_mode: Optional[bool] = None
        self._flight_runner: Optional[FlightRunner] = None
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

    def submission_method(self, parsed_model: Dict[str, Any]) -> str:
        """Decide where a Python model's body runs.

        The per-model `submission_method` config wins, matching how other
        adapters let a single model opt into remote execution; the profile's
        `flights.enabled_by_default` sets the default for the project.
        """
        config = parsed_model.get("config") or {}
        method = config.get("submission_method")
        if method is None:
            flights = self.creds.flights
            return (
                FLIGHT_SUBMISSION if flights and flights.enabled_by_default else LOCAL_SUBMISSION
            )
        method = str(method).lower()
        if method not in SUBMISSION_METHODS:
            raise ValueError(
                f"Unsupported submission_method '{method}' for dbt-duckdb; "
                f"expected one of {', '.join(SUBMISSION_METHODS)}"
            )
        return method

    def flight_runner(self) -> FlightRunner:
        if self._flight_runner is None:
            self._flight_runner = FlightRunner(self.creds.flights or FlightConfig())
        return self._flight_runner

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        if self.submission_method(parsed_model) == FLIGHT_SUBMISSION:
            # The model body runs in MotherDuck's container rather than here,
            # so SaaS mode has nothing to object to.
            return self.flight_runner().submit(handle.cursor(), parsed_model, compiled_code)

        # Block local file access if SaaS mode is on
        if self.motherduck_saas_mode(handle) is True:
            raise RuntimeError(SAAS_MODE_ERROR)
        return super().submit_python_job(
            handle=handle, parsed_model=parsed_model, compiled_code=compiled_code
        )
