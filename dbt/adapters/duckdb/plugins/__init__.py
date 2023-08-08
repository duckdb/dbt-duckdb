import importlib
import os
from typing import Any
from typing import Dict
from typing import Optional

from duckdb import DuckDBPyConnection

from ..utils import SourceConfig
from ..utils import TargetConfig
from dbt.dataclass_schema import dbtClassMixin


class PluginConfig(dbtClassMixin):
    """A helper class for defining the configuration settings a particular plugin uses."""

    pass


class BasePlugin:
    """
    BasePlugin is the base class for creating plugins. A plugin can be created
    from a module name, an optional configuration, and an alias. Each plugin
    contains a name and its configuration.
    """

    # A set of built-in plugins that are included with dbt-duckdb.
    _BUILTIN = set(
        [x.split(".")[0] for x in os.listdir(os.path.dirname(__file__)) if "_" not in x]
    )

    @classmethod
    def create(
        cls,
        module: str,
        *,
        config: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
    ) -> "BasePlugin":
        """
        Create a plugin from a module name and optional configuration.

        :param module: A string representing the module name.
        :param config: An optional dictionary with configuration parameters.
        :param alias: An optional string representing the alias name of the module.
        :return: A Plugin instance.
        :raises ImportError: If the module cannot be imported.
        """
        if not isinstance(module, str):
            raise TypeError("Module name must be a string.")

        if module in cls._BUILTIN:
            name = module
            module = f"dbt.adapters.duckdb.plugins.{module}"
        else:
            name = module.split(".")[-1]

        try:
            mod = importlib.import_module(module)
        except ImportError as e:
            raise ImportError(f"Unable to import module '{module}': {e}")

        if not hasattr(mod, "Plugin"):
            raise ImportError(f"Module '{module}' does not have a Plugin class.")
        else:
            return mod.Plugin(alias or name, config or {})

    def __init__(self, name: str, plugin_config: Dict[str, Any]):
        """
        Initialize the BasePlugin instance with a name and its configuration.
        This method should *not* be overriden by subclasses in general; any
        initialization required from the configuration dictionary should be
        defined in the `initialize` method.

        :param name: A string representing the plugin name.
        :param plugin_config: A dictionary representing the plugin configuration.
        """
        self.name = name
        self.initialize(plugin_config)

    def initialize(self, plugin_config: Dict[str, Any]):
        """
        Initialize the plugin with its configuration dictionary specified in the
        profile. This function may be overridden by subclasses that have
        additional initialization steps.

        :param plugin_config: A dictionary representing the plugin configuration.
        """
        pass

    def configure_connection(self, conn: DuckDBPyConnection):
        """
        Configure the DuckDB connection with any necessary extensions and/or settings.
        This method should be overridden by subclasses to provide additional
        configuration needed on the connection, such as user-defined functions.

        :param conn: A DuckDBPyConnection instance to be configured.
        """
        pass

    def load(self, source_config: SourceConfig):
        """
        Load data from a source config and return it as a DataFrame-like object
        that DuckDB can read. This method should be overridden by subclasses that
        support loading data from a source config.

        :param source_config: A SourceConfig instance representing the source data.
        :raises NotImplementedError: If this method is not implemented by a subclass.
        """
        raise NotImplementedError(f"load method not implemented for {self.name}")

    def store(self, target_config: TargetConfig):
        raise NotImplementedError(f"store method not implemented for {self.name}")
