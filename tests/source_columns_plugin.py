import pandas as pd

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig


class Plugin(BasePlugin):
    def load(self, source_config: SourceConfig):
        # A CSV file without a header row: the column names and types come from the
        # columns declared on the source.
        columns = source_config.columns
        assert source_config.as_dict()["columns"] == columns

        names = [column["name"] for column in columns.values()]
        dtypes = {name: str for name, column in columns.items() if column["data_type"] == "text"}
        return pd.read_csv(source_config["path"], header=None, names=names, dtype=dtypes)
