from typing import Any
from typing import Dict

from duckdb import DuckDBPyRelation

from . import BasePlugin
from ..utils import SourceConfig, TargetConfig

# here will be parquet,csv,json implementation, 
# this plugin should be default one if none is specified
# we can change the name of the plugin

class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        pass

    def configure_cursor(self, cursor):
        pass

    def load(self, source_config: SourceConfig):
        pass 

    def default_materialization(self):
        return "view"
    
    def store(self, df: DuckDBPyRelation, target_config: TargetConfig):
        pass
    

