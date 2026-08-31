"""Run dbt Python models on MotherDuck Flights instead of in the dbt process.

A Flight is a single-file Python program that MotherDuck runs in a container of
its own. The code dbt-core generates for a Python model is already
self-contained -- it needs only a DuckDB connection and a function that turns a
relation name into a DataFrame -- so submitting a model to a Flight comes down
to appending a ``main()`` that supplies those two things, and then driving the
Flight lifecycle from the connection the adapter already holds.

The whole lifecycle is expressed as MD_*_FLIGHT SQL table functions, which are
callable on any MotherDuck connection, so this adds no client library and no
new network path. They are permitted in SaaS mode as well, which is what makes
it possible to run Python models there at all: the model body executes in
MotherDuck's container instead of on the dbt host, so SaaS mode's ban on local
filesystem access is not something we have to work around.
"""

import hashlib
import re
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import duckdb
from dbt_common.exceptions import DbtRuntimeError

from ..credentials import FlightConfig
from ..utils import escape_sql_string
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("DuckDB")

# Statuses a run can end in; anything else means it is still going.
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})

# MotherDuck caps Flight source at 200KB and requirements.txt at 20KB.
MAX_SOURCE_BYTES = 200 * 1024
MAX_REQUIREMENTS_BYTES = 20 * 1024

# Flight names are used in the MotherDuck UI and in logs; keep them readable
# and bounded, since we derive them from fully-qualified model names.
MAX_FLIGHT_NAME_LENGTH = 120

# Appended to the model's compiled code to produce the Flight entrypoint. This
# is the remote counterpart of Environment.run_python_job(): dbt-core's codegen
# supplies model()/dbtObj() and dbt-duckdb's py_write_table macro supplies
# materialize(), so all this has to do is connect and wire them together.
#
# The Flight runtime injects MOTHERDUCK_TOKEN, which duckdb.connect("md:")
# picks up on its own.
FLIGHT_ENTRYPOINT = """

# --- dbt-duckdb flight entrypoint (generated) ---
__dbt_settings = {settings!r}


def __dbt_apply_settings(cursor):
    # The profile's `settings` are applied to every cursor the local
    # environment hands a Python model (see Environment.initialize_cursor), so
    # apply them here too; otherwise the same model produces different results
    # depending on where it was submitted.
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
        # A DuckDB relation may reference temporary tables that cannot cross
        # cursor boundaries, so materialize it on the same cursor.
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

    Returns None for lines that are not a plain requirement (pip options, URLs
    without an egg name), which are passed through untouched.
    """
    match = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[|[=<>!~;@]|$)", requirement)
    if not match:
        return None
    return re.sub(r"[-_.]+", "-", match.group(1)).lower()


def _sanitize(value: str) -> str:
    """Reduce a dbt name component to characters that are safe in a Flight name."""
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")


def flight_name(parsed_model: Dict[str, Any], prefix: str = "dbt") -> str:
    """Build a stable Flight name for a model node.

    The name has to be deterministic so that re-running a model updates its
    Flight instead of creating another one, and distinct across targets so that
    dev and prod runs of the same model do not fight over one Flight.
    """
    parts = [
        parsed_model.get("package_name"),
        parsed_model.get("database"),
        parsed_model.get("schema"),
        parsed_model.get("alias") or parsed_model.get("name"),
    ]
    name = "-".join([prefix] + [_sanitize(str(p)) for p in parts if p])
    if len(name) > MAX_FLIGHT_NAME_LENGTH:
        # Keep the tail (the model identifier is the informative part) and add a
        # digest of the full name so truncation cannot collide.
        digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
        keep = MAX_FLIGHT_NAME_LENGTH - len(digest) - 1
        name = f"{name[-keep:]}-{digest}"
    return name


def settings_statements(settings: Optional[Dict[str, Any]]) -> List[str]:
    """Render the profile's `settings` as SET statements for the Flight.

    Mirrors Environment.initialize_cursor: values go in as strings and DuckDB
    casts them to the setting's type.
    """
    return [f"SET {key} = '{escape_sql_string(value)}'" for key, value in (settings or {}).items()]


def build_source(compiled_code: str, settings: Optional[Dict[str, Any]] = None) -> str:
    """Turn a model's compiled Python into a Flight entrypoint."""
    entrypoint = FLIGHT_ENTRYPOINT.format(settings=settings_statements(settings))
    source = compiled_code.lstrip() + entrypoint
    size = len(source.encode("utf-8"))
    if size > MAX_SOURCE_BYTES:
        raise DbtRuntimeError(
            f"Python model is too large to run as a MotherDuck Flight: "
            f"{size} bytes exceeds the {MAX_SOURCE_BYTES} byte limit. "
            "Move the bulk of the code into a package listed in the model's "
            "`packages` config."
        )
    return source


def build_requirements(parsed_model: Dict[str, Any], config: FlightConfig) -> str:
    """Assemble requirements.txt for the Flight.

    A Flight installs its dependencies before main() runs and has no way to
    install more later, so everything the model imports has to be declared up
    front. dbt's `packages` model config is exactly that list -- it is inert in
    the local environment, where the model runs in dbt's own interpreter, but
    it is load-bearing here.
    """
    # Pin duckdb to whatever the local client uses. That version is known to be
    # accepted by MotherDuck (we are talking to it right now), whereas an
    # unpinned install can pick up a newer PyPI release that MotherDuck
    # rejects at connect time.
    #
    # Later sources override earlier ones for the same distribution, so a
    # model's own `packages` beat the profile-wide `flights.requirements`,
    # which in turn beat the default duckdb pin. Emitting both would leave the
    # installer to fail on two conflicting pins for one distribution.
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
            # pip options (-r, --index-url, ...) and anything else we cannot
            # attribute to a distribution: keep in order, do not deduplicate.
            passthrough.append(entry)
        else:
            resolved[name] = entry

    requirements = "\n".join(passthrough + list(resolved.values())) + "\n"
    size = len(requirements.encode("utf-8"))
    if size > MAX_REQUIREMENTS_BYTES:
        raise DbtRuntimeError(
            f"Python model requirements are too large for a MotherDuck Flight: "
            f"{size} bytes exceeds the {MAX_REQUIREMENTS_BYTES} byte limit."
        )
    return requirements


class FlightRunner:
    """Drives the Flight lifecycle for Python model submission.

    One Flight is kept per model node, so each model gets its own run history
    and source-version history in the MotherDuck UI, which is where you go when
    a model fails.
    """

    def __init__(self, config: FlightConfig, settings: Optional[Dict[str, Any]] = None):
        self._config = config
        self._settings = settings
        # Flight name -> id, so repeat models in one dbt invocation skip the
        # lookup. Ids are stable for a Flight's lifetime.
        self._flight_ids: Dict[str, str] = {}

    def submit(self, cursor, parsed_model: Dict[str, Any], compiled_code: str) -> AdapterResponse:
        name = flight_name(parsed_model, self._config.name_prefix)
        source = build_source(compiled_code, self._settings)
        requirements = build_requirements(parsed_model, self._config)

        flight_id = self._upsert_flight(cursor, name, source, requirements)
        run_number = self._start_run(cursor, flight_id)
        logger.debug(f"Flight {name} ({flight_id}) started run {run_number}")

        status, exit_code = self._await_run(cursor, flight_id, run_number, name)
        if status != "SUCCEEDED":
            raise DbtRuntimeError(
                f"Python model failed on MotherDuck Flight '{name}' "
                f"(run {run_number}, status {status}, exit code {exit_code}):\n"
                + self._run_logs(cursor, flight_id, run_number)
            )
        return AdapterResponse(_message="OK")

    # -- lifecycle steps ---------------------------------------------------

    def _upsert_flight(self, cursor, name: str, source: str, requirements: str) -> str:
        """Create the Flight, or update it when its code has changed.

        Every content change creates a new immutable Flight version, so compare
        against the current version first: an unchanged model should not push a
        new version on every dbt run.
        """
        flight_id = self._find_flight(cursor, name)
        if flight_id is None:
            row = self._query_one(
                cursor,
                "SELECT flight_id FROM MD_CREATE_FLIGHT("
                f"name := '{escape_sql_string(name)}', "
                f"source_code := '{escape_sql_string(source)}', "
                f"requirements_txt := '{escape_sql_string(requirements)}'"
                f"{self._optional_create_args()})",
            )
            flight_id = str(row[0])
            self._flight_ids[name] = flight_id
            logger.debug(f"Created MotherDuck Flight {name} ({flight_id})")
            return flight_id

        if self._is_current(cursor, flight_id, source, requirements):
            logger.debug(f"MotherDuck Flight {name} is up to date; reusing it")
            return flight_id

        self._query_one(
            cursor,
            "SELECT flight_id FROM MD_UPDATE_FLIGHT("
            f"flight_id := '{flight_id}', "
            f"source_code := '{escape_sql_string(source)}', "
            f"requirements_txt := '{escape_sql_string(requirements)}'"
            f"{self._optional_create_args()})",
        )
        logger.debug(f"Updated MotherDuck Flight {name} ({flight_id})")
        return flight_id

    def _optional_create_args(self) -> str:
        args = ""
        if self._config.access_token_name:
            token = escape_sql_string(self._config.access_token_name)
            args += f", access_token_name := '{token}'"
        if self._config.max_runtime_sec is not None:
            args += f", max_runtime_sec := {int(self._config.max_runtime_sec)}"
        return args

    def _find_flight(self, cursor, name: str) -> Optional[str]:
        if name in self._flight_ids:
            return self._flight_ids[name]

        # MD_LIST_FLIGHTS pages, and an account can hold many Flights, so walk
        # until we find the name or run out. `limit`/`offset` are reserved
        # words and have to be quoted as named arguments.
        page, offset = 200, 0
        while True:
            rows = self._query_all(
                cursor,
                "SELECT flight_id, flight_name FROM MD_LIST_FLIGHTS("
                f'"limit" := {page}, "offset" := {offset}, owner_only := true)',
            )
            if not rows:
                return None
            for flight_id, flight_name_ in rows:
                self._flight_ids[flight_name_] = str(flight_id)
            if name in self._flight_ids:
                return self._flight_ids[name]
            if len(rows) < page:
                return None
            offset += page

    def _is_current(self, cursor, flight_id: str, source: str, requirements: str) -> bool:
        """Has this Flight already got exactly this code and requirements?

        Every content change mints a new immutable Flight version, so checking
        first keeps an unchanged model from adding a version on each dbt run.
        The current version number has to be fetched separately: MotherDuck's
        table functions reject subqueries in their arguments.
        """
        current = self._query_one(
            cursor, f"SELECT current_version FROM MD_GET_FLIGHT(flight_id := '{flight_id}')"
        )
        if current is None or current[0] is None:
            return False
        row = self._query_one(
            cursor,
            "SELECT source_code, requirements_txt FROM MD_GET_FLIGHT_VERSION("
            f"flight_id := '{flight_id}', version_number := {int(current[0])})",
        )
        if row is None:
            return False
        return row[0] == source and row[1] == requirements

    def _start_run(self, cursor, flight_id: str) -> int:
        row = self._query_one(
            cursor, f"SELECT run_number FROM MD_RUN_FLIGHT(flight_id := '{flight_id}')"
        )
        return int(row[0])

    def _await_run(self, cursor, flight_id: str, run_number: int, name: str):
        """Poll until the run reaches a terminal status.

        Runs are asynchronous: a successful trigger only means the run was
        accepted, so nothing downstream may assume the model is built until the
        status says SUCCEEDED.
        """
        deadline = time.monotonic() + self._config.timeout_sec
        while True:
            row = self._query_one(
                cursor,
                "SELECT status, exit_code FROM MD_GET_FLIGHT_RUN("
                f"flight_id := '{flight_id}', run_number := {run_number})",
            )
            status = str(row[0])
            if status in TERMINAL_STATUSES:
                return status, row[1]
            if time.monotonic() > deadline:
                # Cancel rather than walk away: an abandoned run would keep
                # going and commit the model table well after dbt reported the
                # node as failed, and a retry would race a second run writing
                # the same relation.
                cancelled = self._cancel_run(cursor, flight_id, run_number)
                raise DbtRuntimeError(
                    f"Timed out after {self._config.timeout_sec}s waiting for MotherDuck "
                    f"Flight '{name}' run {run_number} (last status: {status}). "
                    + (
                        "The run was cancelled. "
                        if cancelled
                        else "The run could not be cancelled and may still be going; "
                        "check it in MotherDuck. "
                    )
                    + "Raise `flights.timeout_sec` if the model needs longer."
                )
            time.sleep(self._config.poll_interval_sec)

    def _cancel_run(self, cursor, flight_id: str, run_number: int) -> bool:
        try:
            self._query_one(
                cursor,
                "SELECT * FROM MD_CANCEL_FLIGHT_RUN("
                f"flight_id := '{flight_id}', run_number := {run_number})",
            )
            return True
        except Exception as err:
            # Losing the race against a run that just finished is normal, and a
            # failed cancel must not mask the timeout we are reporting.
            logger.debug(f"Could not cancel flight run {run_number}: {err}")
            return False

    def _run_logs(self, cursor, flight_id: str, run_number: int) -> str:
        """Fetch the tail of a run's logs, for attaching to a failure."""
        try:
            rows = self._query_all(
                cursor,
                "SELECT line FROM MD_GET_FLIGHT_LOGS("
                f"flight_id := '{flight_id}', run_number := {run_number}, "
                f'"limit" := {self._config.log_lines}, "order" := \'desc\') '
                "ORDER BY line_number",
            )
        except Exception as err:  # pragma: no cover - diagnostics only
            return f"(could not read flight logs: {err})"
        return "\n".join(str(row[0]) for row in rows)

    # -- cursor helpers ----------------------------------------------------

    def _query_all(self, cursor, sql: str) -> List[Any]:
        return cursor.execute(sql).fetchall()

    def _query_one(self, cursor, sql: str) -> Any:
        return cursor.execute(sql).fetchone()
