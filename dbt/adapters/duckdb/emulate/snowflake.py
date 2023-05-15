import datetime as dt

from duckdb import typing as t
from duckdb import DuckDBPyConnection

def emulate(db: DuckDBPyConnection) -> DuckDBPyConnection:
    _emulate_types(db)
    _emulate_functions(db)
    return db

def _emulate_types(db: DuckDBPyConnection) -> None:
    try:
        db.execute("CREATE TYPE NUMBER AS DECIMAL")
    except:
        # already exists, no worries
        pass

def _emulate_functions(db: DuckDBPyConnection) -> None:
    def to_date(arg, fmt=None) -> dt.date:
        if isinstance(arg, dt.datetime):
            return arg.date()
        elif isinstance(arg, str):
            if not fmt:
                fmt = "%Y-%m-%d"
            return dt.datetime.strptime(arg, fmt).date()
    db.create_function("to_date", to_date, None, t.DATE)
    db.create_function("date", to_date, None, t.DATE)
    db.create_function("try_to_date", to_date, None, t.DATE, exception_handling="return_null")