import pathlib

import pandas as pd

from . import BasePlugin
from ..utils import SourceConfig


class Plugin(BasePlugin):
    def load(self, source_config: SourceConfig):
        ext_location = source_config["external_location"]
        ext_location = ext_location.format(**source_config.as_dict())
        source_location = pathlib.Path(ext_location.strip("'"))
        sheet_name = source_config.get("sheet_name", 0)
        return pd.read_excel(source_location, sheet_name=sheet_name)
