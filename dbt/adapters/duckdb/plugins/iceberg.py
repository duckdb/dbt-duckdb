from typing import Dict

from pyiceberg import catalog

from . import Plugin
from ..utils import SourceConfig


class IcebergPlugin(Plugin):
    def __init__(self, config: Dict):
        self._catalog = catalog.load_catalog(config.get("catalog"))

    def load(self, source_config: SourceConfig):
        table_format = source_config.meta.get("iceberg_table", "{schema}.{identifier}")
        table_name = table_format.format(**source_config.as_dict())
        table = self._catalog.load_table(table_name)
        scan_keys = {
            "row_filter",
            "selected_fields",
            "case_sensitive",
            "snapshot_id",
            "options",
            "limit",
        }
        scan_config = {k: source_config.meta[k] for k in scan_keys if k in source_config.meta}
        return table.scan(**scan_config).to_arrow()
