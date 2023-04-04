import abc
import importlib
from typing import Any
from typing import Dict

from dbt.contracts.graph.nodes import SourceDefinition


class Plugin(abc.ABC):
    @classmethod
    def create(cls, impl: str, config: Dict[str, Any]) -> "Plugin":
        module_name, class_name = impl.rsplit(".", 1)
        module = importlib.import_module(module_name)
        Class = getattr(module, class_name)
        if not issubclass(Class, Plugin):
            raise TypeError(f"{impl} is not a subclass of Plugin")
        return Class(config)

    @abc.abstractmethod
    def __init__(self, plugin_config: Dict):
        pass

    @abc.abstractmethod
    def load_source(self, source_definition: SourceDefinition) -> str:
        """Load data from a source and return it as a string."""
        pass

    @abc.abstractmethod
    def store_target(self, data, config: Dict) -> None:
        """Store the given data using the provided config dictionary."""
        pass
