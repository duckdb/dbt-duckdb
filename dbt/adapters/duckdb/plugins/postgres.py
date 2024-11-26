from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from duckdb import DuckDBPyConnection

from . import BasePlugin
from dbt.adapters.events.logging import AdapterLogger

PG_EXT = "postgres"


class Plugin(BasePlugin):
    logger = AdapterLogger("DuckDB_PostgresPlugin")

    def __init__(self, name: str, plugin_config: Dict[str, Any]):
        """
        Initialize the Plugin with a name and configuration.
        """
        super().__init__(name, plugin_config)
        self.logger.debug(
            "Plugin __init__ called with name: %s and config: %s", name, plugin_config
        )
        self.initialize(plugin_config)

    def initialize(self, config: Dict[str, Any]):
        """
        Initialize the plugin with the provided configuration.
        """
        self.logger.debug("Initializing PostgreSQL plugin with config: %s", config)

        self._dsn: str = config["dsn"]
        if not self._dsn:
            self.logger.error(
                "Initialization failed: 'dsn' is a required argument for the postgres plugin!"
            )
            raise ValueError("'dsn' is a required argument for the postgres plugin!")

        self._pg_schema: Optional[str] = config.get("pg_schema")  # Can be None
        self._duckdb_alias: str = config.get("duckdb_alias", "postgres_db")
        self._read_only: bool = config.get("read_only", False)
        self._secret: Optional[str] = config.get("secret")
        self._attach_options: Dict[str, Any] = config.get(
            "attach_options", {}
        )  # Additional ATTACH options
        self._settings: Dict[str, Any] = config.get(
            "settings", {}
        )  # Extension settings via SET commands

        self.logger.info(
            "PostgreSQL plugin initialized with dsn='%s', pg_schema='%s', "
            "duckdb_alias='%s', read_only=%s, secret='%s'",
            self._dsn,
            self._pg_schema,
            self._duckdb_alias,
            self._read_only,
            self._secret,
        )

    def configure_connection(self, conn: DuckDBPyConnection):
        """
        Configure the DuckDB connection to attach the PostgreSQL database.
        """
        self.logger.debug("Configuring DuckDB connection for PostgreSQL plugin.")

        conn.install_extension(PG_EXT)
        conn.load_extension(PG_EXT)
        self.logger.info("PostgreSQL extension installed and loaded.")

        # Set any extension settings provided
        self._set_extension_settings(conn)

        # Build and execute the ATTACH command
        attach_stmt = self._build_attach_statement()
        self.logger.debug("Executing ATTACH statement: %s", attach_stmt)
        try:
            conn.execute(attach_stmt)
            self.logger.info("Successfully attached PostgreSQL database with DSN: %s", self._dsn)
        except Exception as e:
            self.logger.error("Failed to attach PostgreSQL database: %s", e)
            raise

    def _set_extension_settings(self, conn: DuckDBPyConnection):
        """
        Set extension settings via SET commands.
        """
        for setting, value in self._settings.items():
            # Quote string values
            if isinstance(value, str):
                value = f"'{value}'"
            elif isinstance(value, bool):
                value = "true" if value else "false"
            set_stmt = f"SET {setting} = {value};"
            self.logger.debug("Setting extension option: %s", set_stmt)
            try:
                conn.execute(set_stmt)
            except Exception as e:
                self.logger.error("Failed to set option %s: %s", setting, e)
                raise

    def _build_attach_statement(self) -> str:
        """
        Build the ATTACH statement for connecting to the PostgreSQL database.
        """
        attach_options: List[Tuple[str, Optional[str]]] = [("TYPE", "POSTGRES")]

        if self._pg_schema:
            attach_options.append(("SCHEMA", f"'{self._pg_schema}'"))

        if self._secret:
            attach_options.append(("SECRET", f"'{self._secret}'"))

        # Additional attach options
        for k, v in self._attach_options.items():
            if isinstance(v, bool):
                v = "true" if v else "false"
            elif isinstance(v, str):
                v = f"'{v}'"
            attach_options.append((k.upper(), v))

        if self._read_only:
            attach_options.append(("READ_ONLY", None))  # No value assigned

        # Convert options to string
        attach_options_str = ", ".join(
            f"{k} {v}" if v is not None else k for k, v in attach_options
        )

        attach_stmt = f"ATTACH '{self._dsn}' AS {self._duckdb_alias} ({attach_options_str});"
        return attach_stmt
