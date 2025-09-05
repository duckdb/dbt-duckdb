import os
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from urllib.parse import urlparse

from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.duckdb.secrets import DEFAULT_SECRET_PREFIX
from dbt.adapters.duckdb.secrets import Secret


@dataclass
class Attachment(dbtClassMixin):
    # The path to the database to be attached (may be a URL)
    path: str

    # The type of the attached database (defaults to duckdb, but may be supported by an extension)
    type: Optional[str] = None

    # An optional alias for the attached database
    alias: Optional[str] = None

    # An optional reference to a secret name from the secret manager
    secret: Optional[str] = None

    # Whether the attached database is read-only or read/write
    read_only: bool = False

    # Arbitrary key-value pairs for additional ATTACH options
    options: Optional[Dict[str, Any]] = None

    # An optional flag to indicate whether the database is a ducklake database,
    # so that the adapter can generate queries that work for ducklake.
    # This is not always necessary when using a ducklake database, but
    # serves more as a hint for cases where the path does not use the ducklake: scheme,
    # which is the case in fully managed ducklake in MotherDuck.
    is_ducklake: Optional[bool] = None

    def to_sql(self) -> str:
        # remove query parameters (not supported in ATTACH)
        parsed = urlparse(self.path)
        path = self.path.replace(f"?{parsed.query}", "")
        base = f"ATTACH IF NOT EXISTS '{path}'"
        if self.alias:
            base += f" AS {self.alias}"

        # Check for conflicts between legacy fields and options dict
        if self.options:
            conflicts = []
            if self.type and "type" in self.options:
                conflicts.append("type")
            if self.secret and "secret" in self.options:
                conflicts.append("secret")
            if self.read_only and "read_only" in self.options:
                conflicts.append("read_only")

            if conflicts:
                raise DbtRuntimeError(
                    f"Attachment option(s) {conflicts} specified in both direct fields and options dict. "
                    f"Please specify each option in only one location."
                )

        # Collect all options, prioritizing direct fields over options dict
        all_options = []

        # Add legacy options for backward compatibility
        if self.type:
            all_options.append(f"TYPE {self.type}")
        elif self.options and "type" in self.options:
            all_options.append(f"TYPE {self.options['type']}")

        if self.secret:
            all_options.append(f"SECRET {self.secret}")
        elif self.options and "secret" in self.options:
            all_options.append(f"SECRET {self.options['secret']}")

        if self.read_only:
            all_options.append("READ_ONLY")
        elif self.options and "read_only" in self.options and self.options["read_only"]:
            all_options.append("READ_ONLY")

        # Add arbitrary options from the options dict (excluding handled ones)
        if self.options:
            handled_keys = {"type", "secret", "read_only"}
            for key, value in self.options.items():
                if key in handled_keys:
                    continue

                # Format the option appropriately
                if isinstance(value, bool):
                    if value:  # Only add boolean options if they're True
                        all_options.append(key.upper())
                elif value is not None:
                    # Quote string values for DuckDB SQL compatibility
                    if isinstance(value, str):
                        # Only quote if not already quoted (single or double quotes)
                        stripped_value = value.strip()
                        if (stripped_value.startswith("'") and stripped_value.endswith("'")) or (
                            stripped_value.startswith('"') and stripped_value.endswith('"')
                        ):
                            all_options.append(f"{key.upper()} {value}")
                        else:
                            all_options.append(f"{key.upper()} '{value}'")
                    else:
                        all_options.append(f"{key.upper()} {value}")

        if all_options:
            joined = ", ".join(all_options)
            base += f" ({joined})"
        return base


@dataclass
class PluginConfig(dbtClassMixin):
    module: str

    alias: Optional[str] = None

    # A plugin-specific set of configuration options
    config: Optional[Dict[str, Any]] = None


@dataclass
class Remote(dbtClassMixin):
    host: str
    port: int
    user: str
    password: Optional[str] = None


@dataclass
class Retries(dbtClassMixin):
    # The number of times to attempt the initial duckdb.connect call
    # (to wait for another process to free the lock on the DB file)
    connect_attempts: int = 1

    # The number of times to attempt to execute a DuckDB query that throws
    # one of the retryable exceptions
    query_attempts: Optional[int] = None

    # The list of exceptions that we are willing to retry on
    retryable_exceptions: List[str] = field(default_factory=lambda: ["IOException"])


@dataclass
class Extension(dbtClassMixin):
    name: str
    repo: str


@dataclass
class DuckDBCredentials(Credentials):
    database: str = "main"
    schema: str = "main"
    path: str = ":memory:"

    # Any connection-time configuration information that we need to pass
    # to DuckDB (e.g., if we need to enable using unsigned extensions)
    config_options: Optional[Dict[str, Any]] = None

    # any DuckDB extensions we want to install and load (httpfs, parquet, etc.)
    extensions: Optional[List[Union[str, Dict[str, str]]]] = None

    # any additional pragmas we want to configure on our DuckDB connections;
    # a list of the built-in pragmas can be found here:
    # https://duckdb.org/docs/sql/configuration
    # (and extensions may add their own pragmas as well)
    settings: Optional[Dict[str, Any]] = None

    # secrets for connecting to cloud services AWS S3, Azure, Cloudfare R2,
    # Google Cloud and Huggingface.
    secrets: Optional[List[Dict[str, Any]]] = None

    # the root path to use for any external materializations that are specified
    # in this dbt project; defaults to "." (the current working directory)
    external_root: str = "."

    # identify whether to use the default credential provider chain for AWS/GCloud
    # instead of statically defined environment variables
    use_credential_provider: Optional[str] = None

    # A list of additional databases that should be attached to the running
    # DuckDB instance to make them available for use in models; see the
    # schema for the Attachment dataclass above for what fields it can contain
    attach: Optional[List[Attachment]] = None

    # A list of filesystems to attach to the DuckDB database via the fsspec
    # interface; see https://duckdb.org/docs/guides/python/filesystems.html
    #
    # Each dictionary entry must have a "fs" entry to indicate which
    # fsspec implementation should be loaded, and then an arbitrary additional
    # number of key-value pairs that will be passed as arguments to the fsspec
    # registry method.
    filesystems: Optional[List[Dict[str, Any]]] = None

    # Used to configure remote environments/connections
    remote: Optional[Remote] = None

    # A list of dbt-duckdb plugins that can be used to customize the
    # behavior of loading source data and/or storing the relations that are
    # created by SQL or Python models; see the plugins module for more details.
    plugins: Optional[List[PluginConfig]] = None

    # Whether to disable transactions when executing SQL statements; this
    # is useful when we would like the resulting DuckDB database file to
    # be as small as possible.
    disable_transactions: bool = False

    # Whether to keep the DuckDB connection open between invocations of dbt
    keep_open: bool = True

    # A list of paths to Python modules that should be loaded into the
    # running Python environment when dbt is invoked; this is useful for
    # loading custom dbt-duckdb plugins or locally defined modules that
    # provide helper functions for dbt Python models.
    module_paths: Optional[List[str]] = None

    # An optional strategy for allowing retries when certain types of
    # exceptions occur on a model run (e.g., IOExceptions that were caused
    # by networking issues)
    retries: Optional[Retries] = None

    # An optional flag to indicate whether the database is a ducklake database,
    # so that the adapter can generate queries that work for ducklake.
    # This is not always necessary when using a ducklake database, but
    # serves more as a hint for cases where the path does not use the ducklake: scheme,
    # which is the case in fully managed ducklake in MotherDuck.
    is_ducklake: Optional[bool] = None

    def __post_init__(self):
        self.settings = self.settings or {}
        self.secrets = self.secrets or []
        self._secrets = []

        # Build set of ducklake database names for efficient lookup
        self._ducklake_dbs = set()

        if self.is_ducklake or "ducklake:" in self.path.lower():
            self._ducklake_dbs.add(self.path_derived_database_name(self.path))

        if self.attach:
            for attachment in self.attach:
                is_ducklake_flag = getattr(attachment, "is_ducklake", None)
                path = getattr(attachment, "path", None)
                alias = getattr(attachment, "alias", None)

                # Detect ducklake by explicit type, or by path scheme. Be lenient on case.
                if (isinstance(is_ducklake_flag, bool) and is_ducklake_flag) or (
                    isinstance(path, str) and "ducklake:" in path.lower()
                ):
                    if alias:
                        self._ducklake_dbs.add(alias)
                    else:
                        self._ducklake_dbs.add(self.path_derived_database_name(path))

        # Add MotherDuck plugin if the path is a MotherDuck database
        # and plugin was not specified in profile.yml
        if self.is_motherduck:
            if self.plugins is None:
                self.plugins = []
            if "motherduck" not in [plugin.module for plugin in self.plugins]:
                self.plugins.append(PluginConfig(module="motherduck"))

        # For backward compatibility, to be deprecated in the future
        if self.use_credential_provider:
            if self.use_credential_provider == "aws":
                self.secrets.append({"type": "s3", "provider": "credential_chain"})
            else:
                raise ValueError(
                    "Unsupported value for use_credential_provider: "
                    + self.use_credential_provider
                )

        if self.secrets:
            self._secrets = [
                Secret.create(
                    secret_type=secret.pop("type"),
                    name=secret.pop("name", f"{DEFAULT_SECRET_PREFIX}{num + 1}"),
                    **secret,
                )
                for num, secret in enumerate(self.secrets)
            ]

    def secrets_sql(self) -> List[str]:
        return [secret.to_sql() for secret in self._secrets]

    @property
    def motherduck_attach(self):
        # Check if any MotherDuck paths are attached
        attach = []
        for attached_db in self.attach or []:
            parsed = urlparse(attached_db.path)
            if self._is_motherduck(parsed.scheme):
                attach.append(attached_db)
        return attach

    @property
    def is_motherduck_attach(self):
        return len(self.motherduck_attach) > 0

    @property
    def is_motherduck(self):
        parsed = urlparse(self.path)
        return self._is_motherduck(parsed.scheme) or self.is_motherduck_attach

    @staticmethod
    def _is_motherduck(scheme: str) -> bool:
        return scheme in {"md", "motherduck"}

    @staticmethod
    def path_derived_database_name(path: Optional[Any]) -> str:
        if path is None or path == ":memory:":
            return "memory"
        parsed = urlparse(str(path))
        base_file = os.path.basename(parsed.path)
        inferred_db_name = os.path.splitext(base_file)[0]
        if DuckDBCredentials._is_motherduck(parsed.scheme):
            if inferred_db_name == "":
                inferred_db_name = "my_db"
        elif parsed.scheme.lower() == "ducklake":
            parsed_ducklake = urlparse(inferred_db_name)
            inferred_db_name = parsed_ducklake.path

        return inferred_db_name

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        path = data.get("path")
        path_db_name = cls.path_derived_database_name(path)

        # Check if the database field matches any attach alias
        attach_aliases = []
        if data.get("attach"):
            for attach_data in data["attach"]:
                if isinstance(attach_data, dict) and attach_data.get("alias"):
                    attach_aliases.append(attach_data["alias"])

        database_from_data = data.get("database")
        database_matches_attach_alias = database_from_data in attach_aliases

        if path_db_name and "database" not in data:
            data["database"] = path_db_name
        elif path_db_name and data["database"] != path_db_name:
            # Allow database name to differ from path_db if it matches an attach alias
            if not data.get("remote") and not database_matches_attach_alias:
                raise DbtRuntimeError(
                    "Inconsistency detected between 'path' and 'database' fields in profile; "
                    f"the 'database' property must be set to '{path_db_name}' to match the 'path'"
                )
        elif not path_db_name:
            raise DbtRuntimeError(
                "Unable to determine target database name from 'path' field in profile"
            )
        return data

    @property
    def unique_field(self) -> str:
        """
        This property returns a unique field for the database connection.
        If the connection is remote, it returns the host and port as a string.
        If the connection is local, it returns the path and external root as a string.
        """
        if self.remote:
            return self.remote.host + str(self.remote.port)
        else:
            return self.path + self.external_root

    @property
    def type(self):
        return "duckdb"

    def _connection_keys(self):
        return (
            "database",
            "schema",
            "path",
            "config_options",
            "extensions",
            "settings",
            "external_root",
            "use_credential_provider",
            "attach",
            "filesystems",
            "remote",
            "plugins",
            "disable_transactions",
        )
