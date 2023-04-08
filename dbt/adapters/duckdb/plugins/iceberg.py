from typing import Dict

from pyiceberg import catalog

from . import Plugin
from ..utils import SourceConfig


class IcebergPlugin(Plugin):
    def __init__(self, config: Dict):
        self._catalog = catalog.load_catalog(config.get("name"), config.get("properties"))

    def load(self, source_config: SourceConfig):
        table_format = source_config.meta.get("iceberg_table", "{schema}.{identifier}")
        table_name = table_format.format(**source_config.as_dict())
        table = self._catalog.load_table(table_name)
        return table.scan().to_arrow()  # TODO: configurable scans
