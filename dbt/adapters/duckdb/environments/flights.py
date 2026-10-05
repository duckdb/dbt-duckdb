"""Run dbt Python models on MotherDuck Flights instead of in the dbt process.
A Flight is a single-file Python program that MotherDuck runs in its own
container. dbt-core's compiled Python model only needs a DuckDB connection and a
function mapping a relation name to a DataFrame, so the Flight source is that
compiled code plus a generated main() supplying both. The lifecycle is expressed
as MD_*_FLIGHT SQL functions, callable on any MotherDuck connection -- including
one in SaaS mode, which is what lets Python models run there at all.
"""

import re
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import duckdb
from dbt_common.exceptions import DbtRuntimeError

from ..constants import FLIGHT_NAME_KEY
from ..credentials import FlightConfig
from ..utils import escape_sql_string
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("DuckDB")

# Anything else means the run is still going.
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})

# MotherDuck's limits on a Flight definition.
MAX_SOURCE_BYTES = 200 * 1024
MAX_REQUIREMENTS_BYTES = 20 * 1024

# Flight names are shown in the MotherDuck UI; keep them bounded.
MAX_FLIGHT_NAME_LENGTH = 120

# Where a failed run's logs live, unless `flights.log_url_template` says otherwise.
DEFAULT_LOG_URL_TEMPLATE = "https://app.motherduck.com/flights/{flight_id}/runs/{run_number}"

# The remote counterpart of Environment.run_python_job(). dbt-core's codegen
# supplies model()/dbtObj() and py_write_table supplies materialize(); the
# runtime injects MOTHERDUCK_TOKEN, which duckdb.connect("md:") picks up.
FLIGHT_ENTRYPOINT = """

# --- dbt-duckdb flight entrypoint (generated) ---
__dbt_settings = {settings!r}


def __dbt_apply_settings(cursor):
    # Match Environment.initialize_cursor, so a model behaves the same wherever
    # it was submitted.
    for statement in __dbt_settings:
        cursor.execute(statement)


def main():
    import duckdb as _duckdb

    con = _duckdb.connect("md:")
    __dbt_apply_settings(con)

    def load_df_function(table_name):
        return con.query(f"select * from {{table_name}}")

    dbt = dbtObj(load_df_function)
    df = model(dbt, con)
    if isinstance(df, _duckdb.DuckDBPyRelation):
        # A relation can reference temp tables that do not cross cursors.
        materialize(df, con)
    else:
        write_cursor = con.cursor()
        __dbt_apply_settings(write_cursor)
        materialize(df, write_cursor)


if __name__ == "__main__":
    main()
"""


def _distribution_name(requirement: str) -> Optional[str]:
    """The distribution a requirements.txt line pins, normalized per PEP 503.
    None for pip options and anything else not attributable to a distribution.
    """
    match = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[|[=<>!~;@]|$)", requirement)
    if not match:
        return None
    return re.sub(r"[-_.]+", "-", match.group(1)).lower()


def sanitize_flight_name(name: str) -> str:
    """Make a name safe and bounded, preserving anything already reasonable."""
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_-")
    if not name:
        raise DbtRuntimeError("The flight_name macro returned an empty Flight name.")
    return name[:MAX_FLIGHT_NAME_LENGTH]


def settings_statements(settings: Optional[Dict[str, Any]]) -> List[str]:
    """The profile's `settings` as SET statements, as initialize_cursor does."""
    return [f"SET {key} = '{escape_sql_string(value)}'" for key, value in (settings or {}).items()]


def build_source(compiled_code: str, settings: Optional[Dict[str, Any]] = None) -> str:
    """Turn a model's compiled Python into a Flight entrypoint."""
    entrypoint = FLIGHT_ENTRYPOINT.format(settings=settings_statements(settings))
    source = compiled_code.lstrip() + entrypoint
    size = len(source.encode("utf-8"))
    if size > MAX_SOURCE_BYTES:
        raise DbtRuntimeError(
            f"Python model is too large to run as a MotherDuck Flight: {size} bytes "
            f"exceeds the {MAX_SOURCE_BYTES} byte limit. Move the bulk of the code "
            "into a package listed in the model's `packages` config."
        )
    return source


def build_requirements(parsed_model: Dict[str, Any], config: FlightConfig) -> str:
    """Assemble requirements.txt for the Flight.
    A Flight installs dependencies before main() runs and cannot install more
    later, so dbt's `packages` model config -- inert locally -- is load-bearing
    here. Later sources win per distribution, so `packages` beats
    `flights.requirements` beats the default duckdb pin; two pins for one
    distribution would just fail the install. duckdb defaults to the local
    client's version, which MotherDuck accepts; an unpinned install can pick up
    a release it rejects at connect time.
    """
    packages: List[str] = [f"duckdb=={config.duckdb_version or duckdb.__version__}"]
    packages.extend(config.requirements or [])
    packages.extend((parsed_model.get("config") or {}).get("packages") or [])

    resolved: Dict[str, str] = {}
    passthrough: List[str] = []
    for package in packages:
        entry = package.strip()
        if not entry:
            continue
        name = _distribution_name(entry)
        if name is None:
            passthrough.append(entry)
        else:
            resolved[name] = entry

    requirements = "\n".join(passthrough + list(resolved.values())) + "\n"
    size = len(requirements.encode("utf-8"))
    if size > MAX_REQUIREMENTS_BYTES:
        raise DbtRuntimeError(
            f"Python model requirements are too large for a MotherDuck Flight: {size} "
            f"bytes exceeds the {MAX_REQUIREMENTS_BYTES} byte limit."
        )
    return requirements


class FlightRunner:
    """Drives the Flight lifecycle for Python model submission.
    One Flight per model node, so each keeps its own run and version history in
    the MotherDuck UI.
    """

    def __init__(self, config: FlightConfig, settings: Optional[Dict[str, Any]] = None):
        self._config = config
        self._settings = settings
        # Flight name -> id, so later models in one dbt run skip the lookup.
        self._flight_ids: Dict[str, str] = {}

    def submit(self, cursor, parsed_model: Dict[str, Any], compiled_code: str) -> AdapterResponse:
        name = self.flight_name(parsed_model)
        source = build_source(compiled_code, self._settings)
        requirements = build_requirements(parsed_model, self._config)

        flight_id = self._upsert_flight(cursor, name, source, requirements)
        run_number = self._start_run(cursor, flight_id)
        logger.debug(f"Flight {name} ({flight_id}) started run {run_number}")

        status, exit_code = self._await_run(cursor, flight_id, run_number, name)
        if status != "SUCCEEDED":
            raise DbtRuntimeError(
                f"Python model failed on MotherDuck Flight '{name}' (run {run_number}, "
                f"status {status}, exit code {exit_code}).\n"
                + self._log_pointer(cursor, flight_id, run_number)
            )
        return AdapterResponse(_message="OK")

    def flight_name(self, parsed_model: Dict[str, Any]) -> str:
        """The name from the flight_name macro, or a fallback when unresolved."""
        name = parsed_model.get(FLIGHT_NAME_KEY)
        if not name:
            # The macro is resolved by the adapter; fall back when a caller
            # invokes the runner directly.
            parts = [
                parsed_model.get("package_name"),
                parsed_model.get("database"),
                parsed_model.get("schema"),
                parsed_model.get("alias") or parsed_model.get("name"),
            ]
            name = "-".join(["dbt"] + [str(part) for part in parts if part])
        return sanitize_flight_name(str(name))

    # -- lifecycle steps ---------------------------------------------------

    def _upsert_flight(self, cursor, name: str, source: str, requirements: str) -> str:
        """Create the Flight, or update it when its code changed.
        Every content change mints a new immutable version, so check first
        rather than versioning on every dbt run.
        """
        flight_id = self._find_flight(cursor, name)
        if flight_id is None:
            return self._create_flight(cursor, name, source, requirements)

        if self._is_current(cursor, flight_id, source, requirements):
            logger.debug(f"MotherDuck Flight {name} is up to date; reusing it")
            return flight_id

        self._execute(
            cursor,
            "SELECT flight_id FROM MD_UPDATE_FLIGHT("
            f"flight_id := '{flight_id}', "
            f"source_code := '{escape_sql_string(source)}', "
            f"requirements_txt := '{escape_sql_string(requirements)}'"
            f"{self._optional_args()})",
        )
        logger.debug(f"Updated MotherDuck Flight {name} ({flight_id})")
        return flight_id

    def _create_flight(self, cursor, name: str, source: str, requirements: str) -> str:
        sql = (
            "SELECT flight_id FROM MD_CREATE_FLIGHT("
            f"name := '{escape_sql_string(name)}', "
            f"source_code := '{escape_sql_string(source)}', "
            f"requirements_txt := '{escape_sql_string(requirements)}'"
            f"{self._optional_args()})"
        )
        try:
            row = self._execute(cursor, sql).fetchone()
        except DbtRuntimeError as err:
            if "already exists" not in str(err):
                raise
            # Either a concurrent dbt run created it between our lookup and
            # this call, or another MotherDuck user owns the name: Flight names
            # are unique per user, and only the owner can run one.
            self._flight_ids.pop(name, None)
            flight_id = self._find_flight(cursor, name)
            if flight_id is None:
                raise DbtRuntimeError(
                    f"A MotherDuck Flight named '{name}' already exists but is not yours, "
                    "so this model cannot run on it. Override the `duckdb__flight_name` "
                    "macro to give this project's Flights a distinct name."
                ) from err
            return self._upsert_flight(cursor, name, source, requirements)

        flight_id = str(row[0])
        self._flight_ids[name] = flight_id
        logger.debug(f"Created MotherDuck Flight {name} ({flight_id})")
        return flight_id

    def _optional_args(self) -> str:
        args = ""
        if self._config.access_token_name:
            args += f", access_token_name := '{escape_sql_string(self._config.access_token_name)}'"
        if self._config.max_runtime_sec is not None:
            args += f", max_runtime_sec := {int(self._config.max_runtime_sec)}"
        return args

    def _find_flight(self, cursor, name: str) -> Optional[str]:
        """Resolve a Flight id by name, among the Flights we own.
        Only the owner can run a Flight, so another user's is of no use to us.
        """
        if name in self._flight_ids:
            return self._flight_ids[name]

        # `limit`/`offset` are reserved words and need quoting as named args.
        page, offset = 200, 0
        while True:
            rows = self._execute(
                cursor,
                "SELECT flight_id, flight_name FROM MD_LIST_FLIGHTS("
                f'"limit" := {page}, "offset" := {offset}, owner_only := true)',
            ).fetchall()
            if not rows:
                return None
            for flight_id, flight_name in rows:
                self._flight_ids[flight_name] = str(flight_id)
            if name in self._flight_ids:
                return self._flight_ids[name]
            if len(rows) < page:
                return None
            offset += page

    def _is_current(self, cursor, flight_id: str, source: str, requirements: str) -> bool:
        # Two queries: MotherDuck's table functions reject subqueries in args.
        current = self._execute(
            cursor, f"SELECT current_version FROM MD_GET_FLIGHT(flight_id := '{flight_id}')"
        ).fetchone()
        if current is None or current[0] is None:
            return False
        row = self._execute(
            cursor,
            "SELECT source_code, requirements_txt FROM MD_GET_FLIGHT_VERSION("
            f"flight_id := '{flight_id}', version_number := {int(current[0])})",
        ).fetchone()
        return bool(row) and row[0] == source and row[1] == requirements

    def _start_run(self, cursor, flight_id: str) -> int:
        row = self._execute(
            cursor, f"SELECT run_number FROM MD_RUN_FLIGHT(flight_id := '{flight_id}')"
        ).fetchone()
        return int(row[0])

    def _await_run(self, cursor, flight_id: str, run_number: int, name: str):
        """Poll until the run is terminal; a trigger only means it was accepted."""
        deadline = time.monotonic() + self._config.timeout_sec
        while True:
            row = self._execute(
                cursor,
                "SELECT status, exit_code FROM MD_GET_FLIGHT_RUN("
                f"flight_id := '{flight_id}', run_number := {run_number})",
            ).fetchone()
            status = str(row[0])
            if status in TERMINAL_STATUSES:
                return status, row[1]
            if time.monotonic() > deadline:
                # Cancel rather than walk away: an abandoned run would commit
                # the model table after dbt had already failed the node.
                cancelled = self._cancel_run(cursor, flight_id, run_number)
                raise DbtRuntimeError(
                    f"Timed out after {self._config.timeout_sec}s waiting for MotherDuck "
                    f"Flight '{name}' run {run_number} (last status: {status}). "
                    + (
                        "The run was cancelled. "
                        if cancelled
                        else "The run could not be cancelled and may still be going. "
                    )
                    + "Raise `flights.timeout_sec` if the model needs longer.\n"
                    + self._log_pointer(cursor, flight_id, run_number)
                )
            time.sleep(self._config.poll_interval_sec)

    def _cancel_run(self, cursor, flight_id: str, run_number: int) -> bool:
        try:
            self._execute(
                cursor,
                "SELECT * FROM MD_CANCEL_FLIGHT_RUN("
                f"flight_id := '{flight_id}', run_number := {run_number})",
            )
            return True
        except Exception as err:
            # Losing the race against a run that just finished is normal, and a
            # failed cancel must not mask the timeout being reported.
            logger.debug(f"Could not cancel flight run {run_number}: {err}")
            return False

    # -- diagnostics -------------------------------------------------------

    def _log_pointer(self, cursor, flight_id: str, run_number: int) -> str:
        """Where to read the run's logs, plus a tail if `log_lines` asks for one.
        A Flight log includes the whole dependency install, so it is not dumped
        into dbt's output by default.
        """
        lines = [f"Logs: {self._log_location(flight_id, run_number)}"]
        if self._config.log_lines:
            lines.append(self._log_tail(cursor, flight_id, run_number))
        return "\n".join(line for line in lines if line)

    def _log_location(self, flight_id: str, run_number: int) -> str:
        template = self._config.log_url_template or DEFAULT_LOG_URL_TEMPLATE
        return template.format(flight_id=flight_id, run_number=run_number)

    def _log_tail(self, cursor, flight_id: str, run_number: int) -> str:
        try:
            rows = self._execute(
                cursor,
                "SELECT line FROM MD_GET_FLIGHT_LOGS("
                f"flight_id := '{flight_id}', run_number := {run_number}, "
                f'"limit" := {self._config.log_lines}, "order" := \'desc\') '
                "ORDER BY line_number",
            ).fetchall()
        except Exception as err:  # pragma: no cover - diagnostics only
            return f"(could not read flight logs: {err})"
        return "\n".join(str(row[0]) for row in rows)

    # -- cursor helper -----------------------------------------------------

    def _execute(self, cursor, sql: str):
        """Run a statement, keeping the access token label out of any error.
        DuckDB errors can echo the statement, and these carry that label.
        """
        try:
            return cursor.execute(sql)
        except Exception as err:
            raise DbtRuntimeError(self._redact(str(err))) from None

    def _redact(self, message: str) -> str:
        token = self._config.access_token_name
        if token:
            message = message.replace(token, "***")
        return message
