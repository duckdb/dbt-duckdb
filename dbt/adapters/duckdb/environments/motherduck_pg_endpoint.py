from dbt_common.exceptions import DbtRuntimeError

from . import Environment
from .. import credentials
from .. import utils
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection


class MotherDuckPgEndpointEnvironment(Environment):
    def __init__(self, credentials: credentials.DuckDBCredentials):
        super().__init__(credentials)
        if not self.creds.use_motherduck_postgres_endpoint:
            raise DbtRuntimeError(
                "MotherDuckPgEndpointEnvironment requires "
                "use_motherduck_postgres_endpoint"
            )

    def handle(self):
        raise DbtRuntimeError(
            "MotherDuck PostgreSQL endpoint support is not implemented yet"
        )

    def submit_python_job(
        self, handle, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        raise DbtRuntimeError(
            "Python models are not supported when "
            "use_motherduck_postgres_endpoint is true"
        )

    def load_source(self, plugin_name: str, source_config: utils.SourceConfig) -> str:
        raise DbtRuntimeError(
            "source plugins are not supported when "
            "use_motherduck_postgres_endpoint is true"
        )

    def store_relation(self, plugin_name: str, target_config: utils.TargetConfig) -> None:
        raise DbtRuntimeError(
            "store plugins are not supported when "
            "use_motherduck_postgres_endpoint is true"
        )

    def get_binding_char(self) -> str:
        return "%s"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @classmethod
    def cancel(cls, connection: Connection):
        pass
