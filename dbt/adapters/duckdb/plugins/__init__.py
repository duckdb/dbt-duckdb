import abc
import importlib
from typing import Any
from typing import Dict

from duckdb import DuckDBPyConnection
from ..utils import SourceConfig
from dbt.dataclass_schema import dbtClassMixin


class PluginConfig(dbtClassMixin):
    """A helper class for defining the configuration settings a particular plugin uses."""

    pass


class Plugin(abc.ABC):
    WELL_KNOWN_PLUGINS = {
        "excel": "dbt.adapters.duckdb.plugins.excel.ExcelPlugin",
        "gsheet": "dbt.adapters.duckdb.plugins.gsheet.GSheetPlugin",
        "iceberg": "dbt.adapters.duckdb.plugins.iceberg.IcebergPlugin",
        "sqlalchemy": "dbt.adapters.duckdb.plugins.sqlalchemy.SQLAlchemyPlugin",
    }

    @classmethod
    def create(cls, impl: str, config: Dict[str, Any]) -> "Plugin":
        module_name, class_name = impl.rsplit(".", 1)
        module = importlib.import_module(module_name)
        Class = getattr(module, class_name)
        if not issubclass(Class, Plugin):
            raise TypeError(f"{impl} is not a subclass of Plugin")
        return Class(config)

    def __init__(self, name: str, plugin_config: Dict):
        self.name = name

    def configure_connection(self, conn: DuckDBPyConnection):
        pass

    def load_source(self, source_config: SourceConfig):
        """Load data from a source config and return it as a DataFrame-like object that DuckDB can read."""
        raise NotImplementedError
