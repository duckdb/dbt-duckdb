import pathlib
from typing import Dict

import pandas as pd

from . import Plugin
from ..utils import SourceConfig


class ExcelPlugin(Plugin):
    def __init__(self, config: Dict):
        self._config = config

    def load(self, source_config: SourceConfig):
        ext_location = source_config.meta["external_location"]
        ext_location = ext_location.format(**source_config.as_dict())
        source_location = pathlib.Path(ext_location.strip("'"))
        sheet_name = source_config.meta.get("sheet_name", 0)
        return pd.read_excel(source_location, sheet_name=sheet_name)
