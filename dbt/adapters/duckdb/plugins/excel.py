import os
from typing import Any
from typing import Dict

import pathlib

import pandas as pd

from . import BasePlugin
from ..utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        # Pass s3 settings to plugin environment
        if 's3_access_key_id' in plugin_config:
            os.environ['AWS_ACCESS_KEY_ID'] = plugin_config.get('s3_access_key_id')
        if 's3_secret_access_key' in plugin_config:
            os.environ['AWS_SECRET_ACCESS_KEY'] = plugin_config.get('s3_secret_access_key')
        if 's3_region' in plugin_config:
            os.environ['AWS_DEFAULT_REGION'] = plugin_config.get('s3_region')

    def load(self, source_config: SourceConfig):
        ext_location = source_config["external_location"]
        ext_location = ext_location.format(**source_config.as_dict())
        if 's3' in ext_location:
            # Possible to add some treatment in the future
            source_location = ext_location
        else:
            source_location = pathlib.Path(ext_location.strip("'"))
        sheet_name = source_config.get("sheet_name", 0)
        return pd.read_excel(source_location, sheet_name=sheet_name)
