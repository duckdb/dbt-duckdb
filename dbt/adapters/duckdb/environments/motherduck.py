import threading
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set

from dbt_common.exceptions import DbtRuntimeError

from .. import credentials
from ..constants import FLIGHT_SUBMISSION
from ..constants import LOCAL_SUBMISSION
from ..constants import SUBMISSION_METHODS
from ..credentials import FlightConfig
from .flights import FlightRunner
from .local import DuckDBConnectionWrapper
from .local import LocalEnvironment
from dbt.adapters.contracts.connection import AdapterResponse


MOTHERDUCK_SAAS_MODE_QUERY = """
SELECT value FROM duckdb_settings() WHERE name = 'motherduck_saas_mode'
"""

SAAS_MODE_ERROR = (
    "Python models are disabled when MotherDuck SaaS Mode is on. Set "
    "`submission_method: flight` on the model (or `flights: {enabled_by_default: true}` "
    "in your profile) to run it on MotherDuck Flights instead."
)


class MotherDuckEnvironment(LocalEnvironment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        self._motherduck_saas_mode: Optional[bool] = None
        self._flight_runner: Optional[FlightRunner] = None
        self._flight_lock = threading.RLock()
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
        """Where a Python model's body runs; the model config beats the profile."""
        config = parsed_model.get("config") or {}
        method = config.get("submission_method")
        if method is None:
            flights = self.creds.flights
            return (
                FLIGHT_SUBMISSION if flights and flights.enabled_by_default else LOCAL_SUBMISSION
            )
        method = str(method).lower()
        if method not in SUBMISSION_METHODS:
            raise DbtRuntimeError(
                f"Unsupported submission_method '{method}' for dbt-duckdb; "
                f"expected one of {', '.join(SUBMISSION_METHODS)}"
            )
        return method

    def flight_databases(self) -> Set[str]:
        """Database names that resolve both locally and inside a Flight.
        A Flight's fresh `md:` handle only knows MotherDuck databases, under
        their real names -- so an aliased attachment is unusable remotely even
        though it points at MotherDuck.
        """
        names: Set[str] = set()
        if self.creds.is_motherduck_database:
            names.add(self.creds.database)
        for attachment in self.creds.motherduck_attach:
            remote_name = self.creds.path_derived_database_name(attachment.path)
            if attachment.alias in (None, remote_name):
                names.add(remote_name)
        return names

    def validate_flight_target(self, parsed_model: Dict[str, Any]) -> None:
        """Reject models a Flight could not build, before paying for a run.
        This environment is also used when MotherDuck is merely attached to a
        local database, where a model can target a dbt-process-only catalog.
        """
        databases = self.flight_databases()
        database = parsed_model.get("database")
        if database in databases:
            return
        if not databases:
            raise DbtRuntimeError(
                "Python models can only be submitted to MotherDuck Flights when the "
                "connection targets a MotherDuck database. This profile's `path` is "
                f"'{self.creds.path}'. Point `path` at a MotherDuck database, or use "
                "`submission_method: local`."
            )
        raise DbtRuntimeError(
            f"Python model targets database '{database}', which is not reachable from a "
            "MotherDuck Flight; a Flight can only see MotherDuck databases under their "
            f"real names ({', '.join(sorted(databases))}). Use `submission_method: local` "
            "for this model."
        )

    def flight_runner(self) -> FlightRunner:
        # Without the lock, concurrent dbt threads each build a runner and lose
        # the others' flight-id cache.
        with self._flight_lock:
            if self._flight_runner is None:
                self._flight_runner = FlightRunner(
                    self.creds.flights or FlightConfig(), self.creds.settings
                )
            return self._flight_runner

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        if self.submission_method(parsed_model) == FLIGHT_SUBMISSION:
            # Runs in MotherDuck's container, so SaaS mode has no objection.
            self.validate_flight_target(parsed_model)
            return self.flight_runner().submit(handle.cursor(), parsed_model, compiled_code)

        # Block local file access if SaaS mode is on
        if self.motherduck_saas_mode(handle) is True:
            raise RuntimeError(SAAS_MODE_ERROR)
        return super().submit_python_job(
            handle=handle, parsed_model=parsed_model, compiled_code=compiled_code
        )
