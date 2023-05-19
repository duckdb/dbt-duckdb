import datetime as dt

from duckdb import typing as t
from duckdb import DuckDBPyConnection
from sqlglot.dialects.snowflake import Snowflake

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
            else:
                fmt = convert_snowflake_format(fmt)
            return dt.datetime.strptime(arg, fmt).date()
        else:
            raise Exception("Unsupported type for to_date: {}".format(type(arg)))

    db.create_function("to_date", to_date, None, t.DATE)
    db.create_function("date", to_date, None, t.DATE)
    db.create_function(
        "try_to_date", to_date, None, t.DATE, exception_handling="return_null"
    )


def convert_snowflake_format(snowflake_format_str):
    format_mapping = Snowflake.time_mapping
    python_format_str = snowflake_format_str
    for snowflake_str, python_str in format_mapping.items():
        python_str = python_str.replace("-", "")
        python_format_str = python_format_str.replace(snowflake_str, python_str)
    return python_format_str
