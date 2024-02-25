import os
import pathlib
from typing import Any
from typing import Dict

import pandas as pd
from duckdb import DuckDBPyRelation
from pandas.io.formats import excel

from . import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config

        # Pass s3 settings to plugin environment
        if "s3_access_key_id" in plugin_config:
            os.environ["AWS_ACCESS_KEY_ID"] = plugin_config["s3_access_key_id"]
        if "s3_secret_access_key" in plugin_config:
            os.environ["AWS_SECRET_ACCESS_KEY"] = plugin_config["s3_secret_access_key"]
        if "s3_region" in plugin_config:
            os.environ["AWS_DEFAULT_REGION"] = plugin_config["s3_region"]

    def load(self, source_config: SourceConfig, coursor=None):
        ext_location = source_config["external_location"]
        ext_location = ext_location.format(**source_config.as_dict())
        if "s3" in ext_location:
            # Possible to add some treatment in the future
            source_location = ext_location
        else:
            source_location = pathlib.Path(ext_location.strip("'"))
        sheet_name = source_config.get("sheet_name", 0)
        return pd.read_excel(source_location, sheet_name=sheet_name)

    def store(self, df: DuckDBPyRelation, target_config: TargetConfig, cursor=None):
        plugin_output_config = self._config["output"]

        # this writer doesnt take location but always something defined in the profile?
        _excel_writer = pd.ExcelWriter(
            target_config.location.path,
            mode=plugin_output_config.get("mode", "w"),
            engine=plugin_output_config.get("engine", "xlsxwriter"),
            engine_kwargs=plugin_output_config.get("engine_kwargs", {}),
            date_format=plugin_output_config.get("date_format"),
            datetime_format=plugin_output_config.get("datetime_format"),
        )
        if not plugin_output_config.get("header_styling", True):
            excel.ExcelFormatter.header_style = None

        target_output_config = {
            **plugin_output_config,
            **target_config.config.get("overrides", {}),
        }

        if "sheet_name" not in target_output_config:
            # Excel sheet name is limited to 31 characters
            sheet_name = (target_config.relation.identifier or "Sheet1")[0:31]
            target_output_config["sheet_name"] = sheet_name

        pd_df = df.df()  # duckdb model to pandas dataframe
        if target_output_config.get("skip_empty_sheet", False) and df.shape[0] == 0:
            return
        try:
            pd_df.to_excel(
                _excel_writer,
                sheet_name=target_output_config["sheet_name"],
                na_rep=target_output_config.get("na_rep", ""),
                float_format=target_output_config.get("float_format", None),
                header=target_output_config.get("header", True),
                index=target_output_config.get("index", True),
                merge_cells=target_output_config.get("merge_cells", True),
                inf_rep=target_output_config.get("inf_rep", "inf"),
            )
        except ValueError as ve:
            # Catches errors resembling the below & logs an appropriate message
            # ValueError('This sheet is too large! Your sheet size is: 1100000, 1 Max sheet size is: 1048576, 16384')
            if (
                str(ve).startswith("This sheet is too large")
                and target_output_config["ignore_sheet_too_large"]
            ):
                pd.DataFrame(
                    [{"Error": target_output_config.get("ignore_sheet_too_large_error", str(ve))}]
                ).to_excel(
                    _excel_writer,
                    sheet_name=target_output_config["sheet_name"],
                    index=False,
                )
            else:
                raise ve

        _excel_writer.close()

    def create_source_config(self, target_config: TargetConfig) -> SourceConfig:
        # in the reader we have just location and sheet_name, maybe we can add here more options
        # but in the first place i would not recommend to upstream excel file
        # this works for a very simple case but not all of them
        meta = {
            "external_location": target_config.location.path,
            "sheet_name": target_config.config.get("sheet_name", 0),
        }

        source_config = SourceConfig(
            name=target_config.relation.name,
            identifier=target_config.relation.identifier,
            schema=target_config.relation.schema,
            database=target_config.relation.database,
            meta=meta,
            tags=[],
        )
        return source_config

    def can_be_upstream_referenced(self):
        return True

    def adapt_target_config(self, target_config: TargetConfig) -> TargetConfig:
        if target_config.location.format == "default":
            target_config.location.format = "xlsx"

        target_config.location.path = (
            target_config.location.path + "." + target_config.location.format
        )

        return target_config
