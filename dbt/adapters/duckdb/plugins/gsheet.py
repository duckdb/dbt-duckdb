from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Literal

import gspread
import pandas as pd

from . import BasePlugin
from . import PluginConfig
from ..utils import SourceConfig


@dataclass
class GSheetConfig(PluginConfig):
    method: Literal["service", "oauth"]

    def client(self):
        if self.method == "service":
            return gspread.service_account()
        else:
            return gspread.oauth()


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self._config = GSheetConfig.from_dict(config)
        self._gc = self._config.client()

    def load(self, source_config: SourceConfig):
        doc = None
        if "title" in source_config:
            doc = self._gc.open(source_config["title"])
        elif "key" in source_config:
            doc = self._gc.open_by_key(source_config["key"])
        elif "url" in source_config:
            doc = self._gc.open_by_url(source_config["url"])
        else:
            raise Exception("Source config did not indicate a method to open a GSheet to read")

        sheet = None
        if "worksheet" in source_config:
            work_id = source_config["worksheet"]
            if isinstance(work_id, int):
                sheet = doc.get_worksheet(work_id)
            elif isinstance(work_id, str):
                sheet = doc.worksheet(work_id)
            else:
                raise Exception(
                    f"Could not identify a worksheet in the doc from identifier: {work_id}"
                )
        else:
            sheet = doc.sheet1

        return pd.DataFrame(sheet.get_all_records())
