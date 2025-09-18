import os
import pathlib
from threading import Lock
from typing import Any
from typing import Dict

import pandas as pd
from pandas.io.formats import excel

from . import BasePlugin
from . import pd_utils
from ..utils import SourceConfig
from ..utils import TargetConfig
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("DuckDB")


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config

        if "output" in plugin_config:
            self._excel_writer_create_lock = Lock()
            assert isinstance(plugin_config["output"], dict)
            assert "file" in plugin_config["output"]

        # Pass s3 settings to plugin environment
        if "s3_access_key_id" in plugin_config:
            os.environ["AWS_ACCESS_KEY_ID"] = plugin_config["s3_access_key_id"]
        if "s3_secret_access_key" in plugin_config:
            os.environ["AWS_SECRET_ACCESS_KEY"] = plugin_config["s3_secret_access_key"]
        if "s3_region" in plugin_config:
            os.environ["AWS_DEFAULT_REGION"] = plugin_config["s3_region"]

    def load(self, source_config: SourceConfig):
        ext_location = source_config["external_location"]
        ext_location = ext_location.format(**source_config.as_dict())
        if "s3" in ext_location:
            # Possible to add some treatment in the future
            source_location = ext_location
        else:
            source_location = pathlib.Path(ext_location.strip("'"))
        sheet_name = source_config.get("sheet_name", 0)
        return pd.read_excel(source_location, sheet_name=sheet_name)

    def store(self, target_config: TargetConfig):
        plugin_output_config = self._config["output"]

        # Create the writer on the first instance of the call to store.
        # Instead if we instantiated the writer in the constructor
        # with mode = 'w', this would result in an existing file getting
        # overwritten. This can happen if dbt test is executed for example.
        if not hasattr(self, "_excel_writer"):
            with self._excel_writer_create_lock:
                if not hasattr(self, "_excel_writer"):
                    self._excel_writer = pd.ExcelWriter(
                        plugin_output_config["file"],
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

        df = pd_utils.target_to_df(target_config)
        if (
            target_output_config.get("skip_empty_sheet", False)
            and target_config.location
            and target_config.location.path
        ):
            # This option instructs this plugin to avoid creating a sheet which would contain
            # no data. To perform said check for no data, we need to generate a DF with data
            # only. If the output path contains partitions, those partitions will be treated as
            # columns within the DF, skewing our check. Therefore we parse the target location
            # to detect these columns and drop them. Given a location like,
            # s3://example/datasets/model/_version=1758152450.367933/data_0.parquet,
            # this would drop the _version column.
            target_location = target_config.location.path
            partitions = [t for t in target_location.split("/") if "=" in t]
            partition_columns = [p.split("=")[0] for p in partitions]
            data_df = df.drop(columns=partition_columns)
            data_df = data_df[data_df.notna().any(axis=1)]
            if data_df.shape[0] == 0:
                return
        try:
            df.to_excel(
                self._excel_writer,
                sheet_name=target_output_config["sheet_name"],
                na_rep=target_output_config.get("na_rep", ""),
                float_format=target_output_config.get("float_format", None),
                header=target_output_config.get("header", True),
                index=target_output_config.get("index", True),
                merge_cells=target_output_config.get("merge_cells", True),
                inf_rep=target_output_config.get("inf_rep", "inf"),
            )
            if not target_output_config.get("lazy_close", True):
                self._excel_writer.close()
                del self._excel_writer
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
                    self._excel_writer, sheet_name=target_output_config["sheet_name"], index=False
                )
            else:
                raise ve

    def __del__(self):
        if hasattr(self, "_excel_writer"):
            logger.info(f"Closing {self._config['output']['file']}")
            self._excel_writer.close()
