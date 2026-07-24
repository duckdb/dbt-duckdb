import os
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional

import gspread
import pandas as pd

from . import BasePlugin
from . import PluginConfig
from ..utils import SourceConfig

DEFAULT_IMPERSONATE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


@dataclass
class GSheetConfig(PluginConfig):
    method: Literal["service", "oauth"]
    keyfile: Optional[str] = None
    impersonate: Optional[str] = None
    scopes: Optional[List[str]] = None

    def client(self):
        if self.method == "oauth":
            return gspread.oauth()

        if self.impersonate:
            from google.oauth2.service_account import Credentials

            keyfile = os.path.expanduser(
                self.keyfile or gspread.auth.DEFAULT_SERVICE_ACCOUNT_FILENAME
            )
            scopes = self.scopes or DEFAULT_IMPERSONATE_SCOPES
            creds = Credentials.from_service_account_file(keyfile, scopes=scopes).with_subject(
                self.impersonate
            )
            return gspread.authorize(creds)

        if self.keyfile:
            return gspread.service_account(filename=os.path.expanduser(self.keyfile))
        return gspread.service_account()


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

        if "range" in source_config:
            range = source_config["range"]
            df = pd.DataFrame(sheet.get(range))
            if "headers" in source_config:
                headers = source_config["headers"]
                if len(headers) == len(df.columns):
                    df.columns = headers
                    return df
                else:
                    raise Exception(
                        f"Number of configured headers ({len(headers)}) does not match number of columns in fetched range ({len(df.columns)})."
                    )
            else:
                return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)

        else:
            return pd.DataFrame(sheet.get_all_records())
