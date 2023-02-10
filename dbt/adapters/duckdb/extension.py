from abc import ABC
from abc import abstractmethod
from typing import Dict


class BaseExtension(ABC):
    def __init__(self, name, connection, params=None):
        self.name = name
        self.connection = connection
        self.params = params

    def install(self):
        self.connection.execute(f"INSTALL '{self.name}'")

    def load(self):
        self.connection.execute(f"LOAD '{self.name}'")

    @abstractmethod
    def configure(self):
        pass


class HttpFsExtension(BaseExtension):
    def configure(self):
        pass


class S3Extension(BaseExtension):
    def configure(self):
        pass


class ParquetExtension(BaseExtension):
    def configure(self):
        pass


class SQLiteExtension(BaseExtension):
    def configure(self):
        is_attached = self.connection.execute(
            "select * from information_schema.tables where table_name = '_SqliteDatabaseProperties'"
        ).fetchone()
        if not is_attached:
            self.connection.execute(f"CALL sqlite_attach('{self.params['path']}')")


class PostgresExtension(BaseExtension):
    def configure(self):
        self.connection.execute(
            f"CALL postgres_attach('dbname={self.params['db_name']}"
            f"user={self.params['user']} host={self.params['host']}',"
            f"source_schema='{self.params['source_schema']}',"
            f"sink_schema='{self.params['sink_schema']}')"
        )


class ExtensionFactory:
    EXTENSIONS = {
        "sqlite": SQLiteExtension,
        "postgres": PostgresExtension,
        "httpfs": HttpFsExtension,
        "s3": S3Extension,
        "parquet": ParquetExtension,
    }

    @staticmethod
    def register_extension(extension_info, connection):
        """
        Initialize an extension object based on the extension name and parameters. The extension_info can be a simple
        string or a dictionary with the extension name as the key and the parameters as the value.
        """
        if isinstance(extension_info, Dict):
            extension_name = list(extension_info.keys())[0]
            extension_params = extension_info[extension_name]
        else:
            extension_name = extension_info
            extension_params = {}
        if extension_name not in ExtensionFactory.EXTENSIONS:
            raise ValueError(
                f"Extension type {extension_name} not supported."
                f"Supported extensions are: {ExtensionFactory.EXTENSIONS.keys()}"
            )
        return ExtensionFactory.EXTENSIONS[extension_name](
            extension_name, connection, extension_params
        )
