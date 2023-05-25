import importlib
from typing import Any
from typing import Dict
from typing import Optional

from duckdb import DuckDBPyConnection

from ..utils import SourceConfig
from dbt.dataclass_schema import dbtClassMixin


class PluginConfig(dbtClassMixin):
    """A helper class for defining the configuration settings a particular plugin uses."""

    pass


class BasePlugin:
    @classmethod
    def create(
        cls, module: str, *, config: Optional[Dict[str, Any]] = None, alias: Optional[str] = None
    ) -> "BasePlugin":
        """Create a plugin from a module name and optional configuration."""
        if "." not in module:
            name = module
            module = f"dbt.adapters.duckdb.plugins.{module}"
        else:
            name = module.split(".")[-1]
        mod = importlib.import_module(module)
        return mod.Plugin(alias or name, config or {})

    def __init__(self, name: str, plugin_config: Dict[str, Any]):
        self.name = name
        self.initialize(plugin_config)

    def initialize(self, plugin_config: Dict[str, Any]):
        """Initialize the plugin with its configuration dictionary."""
        pass

    def configure_connection(self, conn: DuckDBPyConnection):
        """Configure the DuckDB connection with any necessary extensions and/or settings."""
        pass

    def load(self, source_config: SourceConfig):
        """Load data from a source config and return it as a DataFrame-like object that DuckDB can read."""
        raise NotImplementedError(f"load_source not implemented for {self.name}")
