import dbt.exceptions
import duckdb

from .credentials import Attachment, DuckDBCredentials


class DuckDBCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor

    # forward along all non-execute() methods/attribute look ups
    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                return self._cursor.execute(sql)
            else:
                return self._cursor.execute(sql, bindings)
        except RuntimeError as e:
            raise dbt.exceptions.DbtRuntimeError(str(e))


class DuckDBConnectionWrapper:
    def __init__(self, env):
        self._env = env
        self._cursor = DuckDBCursorWrapper(env.cursor())

    def close(self):
        self._env.close(self._cursor)

    def cursor(self):
        return self._cursor


class Environment:
    def handle(self):
        raise NotImplementedError

    def cursor(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class LocalEnvironment(Environment):
    def __init__(self, credentials: DuckDBCredentials):
        self.creds = credentials
        self.handles = 0
        self.conn = duckdb.connect(credentials.path, read_only=False)
        # install any extensions on the connection
        if credentials.extensions is not None:
            for extension in credentials.extensions:
                self.conn.execute(f"INSTALL '{extension}'")

        # Attach any fsspec filesystems on the database
        if credentials.filesystems:
            import fsspec

            for spec in credentials.filesystems:
                curr = spec.copy()
                fsimpl = curr.pop("fs")
                fs = fsspec.filesystem(fsimpl, **curr)
                self.conn.register_filesystem(fs)

        # attach any databases that we will be using
        if credentials.attach:
            for entry in credentials.attach:
                attachment = Attachment(**entry)
                self.conn.execute(attachment.to_sql())

    def handle(self):
        self.handles += 1
        return DuckDBConnectionWrapper(self)

    def cursor(self):
        # Extensions/settings need to be configured per cursor
        cursor = self.conn.cursor()
        for ext in self.creds.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in self.creds.load_settings().items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        return cursor

    def close(self, cursor):
        cursor.close()
        self.handles -= 1
        if self.conn and self.handles == 0 and self.creds.path != ":memory:":
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.conn.close()
        self.conn = None


def create(creds: DuckDBCredentials) -> Environment:
    return LocalEnvironment(creds)
