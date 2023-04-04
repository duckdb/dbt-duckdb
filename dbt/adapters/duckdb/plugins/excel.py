from typing import Dict

import pandas as pd

from . import Plugin


class ExcelPlugin(Plugin):
    def __init__(self, config: Dict):
        self._config = config

    def load_source(self, source_definition) -> str:
        var_name = f"__excel_source_{source_definition.identifier}"
        globals()[var_name] = pd.read_excel()
        return var_name
