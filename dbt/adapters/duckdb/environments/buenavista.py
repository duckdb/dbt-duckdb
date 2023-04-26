import json

import psycopg2

from . import Environment
from .. import credentials
from .. import utils
from dbt.contracts.connection import AdapterResponse


class BVEnvironment(Environment):
    @classmethod
    def _get_conn(cls, dbname: str, remote: credentials.Remote):
        return psycopg2.connect(
            dbname=dbname,
            user=remote.user,
            host=remote.host,
            port=remote.port,
        )

    def __init__(self, credentials: credentials.DuckDBCredentials):
        self.creds = credentials
        if not self.creds.remote:
            raise Exception("BVConnection only works with a remote host")

    def handle(self):
        # Extensions/settings need to be configured per cursor
        conn = self._get_conn(self.creds.database, self.creds.remote)
        cursor = self.initialize_cursor(self.creds, conn.cursor())
        cursor.close()
        return conn

    def get_binding_char(self) -> str:
        return "%s"

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        identifier = parsed_model["alias"]
        payload = {
            "method": "dbt_python_job",
            "params": {
                "module_name": identifier,
                "module_definition": compiled_code,
            },
        }
        # TODO: handle errors here
        handle.cursor().execute(json.dumps(payload))
        return AdapterResponse(_message="OK")

    def load_source(self, plugin_name: str, source_config: utils.SourceConfig):
        handle = self.handle()
        payload = {
            "method": "dbt_load_source",
            "params": {
                "plugin_name": plugin_name,
                "source_config": source_config.as_dict(),
            },
        }
        cursor = handle.cursor()
        cursor.execute(json.dumps(payload))
        cursor.close()
        handle.close()
