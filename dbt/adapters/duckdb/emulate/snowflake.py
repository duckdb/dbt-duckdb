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
    format_mapping = {
        "yyyy": "%Y",
        "yy": "%y",
        "mmmm": "%B",
        "mm": "%m",
        "mon": "%b",
        "dd": "%d",
        "dy": "%a",
        "hh24": "%H",
        "hh12": "%I",
        "mi": "%M",
        "ss": "%S",
        # The following don't have a direct equivalent in Python
        # "FF[0-9]": ???,
        # "TZH:TZM" : ???,
        # "TZHTZM" : ???,
        # "TZH" : ???,
    }
    python_format_str = snowflake_format_str.lower()
    for snowflake_str, python_str in format_mapping.items():
        python_format_str = python_format_str.replace(snowflake_str, python_str)
    unsupported_formats = ["ff[0-9]", "tzh:tzm", "tzhtzm", "tzh"]
    for unsupported in unsupported_formats:
        if unsupported in python_format_str:
            raise ValueError(f"Unsupported Snowflake format: {unsupported}")
    return python_format_str
