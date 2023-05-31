from typing import Any
from typing import Dict

import pyiceberg.catalog

from . import BasePlugin
from ..utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        if "catalog" not in config:
            raise Exception("'catalog' is a required argument for the iceberg plugin!")
        catalog = config.pop("catalog")
        self._catalog = pyiceberg.catalog.load_catalog(catalog, **config)

    def load(self, source_config: SourceConfig):
        table_format = source_config.get("iceberg_table", "{schema}.{identifier}")
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
        scan_config = {k: source_config[k] for k in scan_keys if k in source_config}
        return table.scan(**scan_config).to_arrow()
