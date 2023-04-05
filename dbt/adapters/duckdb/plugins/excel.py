from typing import Dict

import pandas as pd

from . import Plugin


class ExcelPlugin(Plugin):
    def __init__(self, config: Dict):
        self._config = config

    def load_data(self, source_config) -> pd.DataFrame:
        return pd.read_csv()
