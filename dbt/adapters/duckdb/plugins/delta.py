from __future__ import annotations

from enum import Enum
from typing import Any
from typing import Dict

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake import write_deltalake
from deltalake._internal import TableNotFoundError

from . import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig


class WriteModes(str, Enum):
    """Enum class for the write modes supported by the plugin."""

    OVERWRITE_PARTITION = "overwrite_partition"
    MERGE = "merge"
    OVERWRITE = "overwrite"


class PartitionKeyMissingError(Exception):
    """Exception raised when the partition key is missing from the target configuration."""

    pass


class UniqueKeyMissingError(Exception):
    """Exception raised when the unique key is missing from the target configuration."""

    pass


class DeltaTablePathMissingError(Exception):
    """Exception raised when the delta table path is missing from the source configuration."""

    pass


def delta_table_exists(table_path: str, storage_options: dict) -> bool:
    """Check if a delta table exists at the given path."""
    try:
        DeltaTable(table_path, storage_options=storage_options)
    except TableNotFoundError:
        return False
    return True


def create_insert_partition(
    table_path: str, data: pa.lib.Table, partitions: list, storage_options: dict
) -> None:
    """Create a new delta table with partitions or overwrite an existing one."""

    if delta_table_exists(table_path, storage_options):
        partition_expr = [
            (partition_name, "=", partition_value)
            for (partition_name, partition_value) in partitions
        ]
        print(
            f"Overwriting delta table under: {table_path} \nwith partition expr: {partition_expr}"
        )
        write_deltalake(table_path, data, partition_filters=partition_expr, mode="overwrite")
    else:
        partitions = [partition_name for (partition_name, partition_value) in partitions]
        print(f"Creating delta table under: {table_path} \nwith partitions: {partitions}")
        write_deltalake(table_path, data, partition_by=partitions)


def delta_write(
    mode: WriteModes,
    table_path: str,
    df: pa.lib.Table,
    storage_options: dict,
    partition_key: str | list[str],
    unique_key: str | list[str],
):
    if storage_options is None:
        storage_options = {}

    if mode == WriteModes.OVERWRITE_PARTITION:
        if not partition_key:
            raise PartitionKeyMissingError(
                "'partition_key' has to be defined for mode 'overwrite_partition'!"
            )

        if isinstance(partition_key, str):
            partition_key = [partition_key]

        partition_dict = []
        # TODO: Add support overwriting multiple partitions
        for each_key in partition_key:
            unique_key_array = pc.unique(df[each_key])

            if len(unique_key_array) == 1:
                partition_dict.append((each_key, str(unique_key_array[0])))
            else:
                raise Exception(
                    f"'{each_key}' column has not one unique value, values are: {str(unique_key_array)}"
                )
        create_insert_partition(table_path, df, partition_dict, storage_options)
    elif mode == WriteModes.MERGE:
        if not unique_key:
            raise UniqueKeyMissingError("'unique_key' has to be defined when mode 'merge'!")
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        predicate_stm = " and ".join(
            [
                f'source."{each_unique_key}" = target."{each_unique_key}"'
                for each_unique_key in unique_key
            ]
        )

        if not delta_table_exists(table_path, storage_options):
            write_deltalake(table_or_uri=table_path, data=df, storage_options=storage_options)

        target_dt = DeltaTable(table_path, storage_options=storage_options)
        # TODO there is a problem if the column name is uppercase
        target_dt.merge(
            source=df,
            predicate=predicate_stm,
            source_alias="source",
            target_alias="target",
        ).when_not_matched_insert_all().execute()
    elif mode == WriteModes.OVERWRITE:
        write_deltalake(
            table_or_uri=table_path,
            data=df,
            mode="overwrite",
            storage_options=storage_options,
        )
    else:
        raise NotImplementedError(f"Mode {mode} not supported!")

    # TODO: Add support for OPTIMIZE


def delta_load(table_path: str, storage_options: dict, as_of_version: int, as_of_datetime: str):
    """Load a delta table as a pyarrow dataset."""
    dt = DeltaTable(table_path, storage_options=storage_options)

    if as_of_version:
        dt.load_version(as_of_version)

    if as_of_datetime:
        dt.load_with_datetime(as_of_datetime)

    return dt.to_pyarrow_dataset()


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        pass

    def configure_cursor(self, cursor):
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise DeltaTablePathMissingError(
                "'delta_table_path' is a required argument for the delta table!"
            )

        # Get required variables from the source configuration
        table_path = source_config["delta_table_path"]

        # Get optional variables from the source configuration
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)
        storage_options = source_config.get("storage_options", {})

        df = delta_load(
            table_path=table_path,
            storage_options=storage_options,
            as_of_version=as_of_version,
            as_of_datetime=as_of_datetime,
        )

        return df

    def default_materialization(self):
        return "view"

    def store(self, target_config: TargetConfig, df: pa.lib.Table = None):
        # Assert that the target_config has a location and relation identifier
        assert target_config.location is not None, "Location is required for storing data!"

        # Get required variables from the target configuration
        table_path = target_config.location.path

        # Get optional variables from the target configuration
        mode = target_config.config.get("mode", "overwrite")
        storage_options = target_config.config.get("storage_options", {})
        partition_key = target_config.config.get("partition_key", None)
        unique_key = target_config.config.get("unique_key", None)

        delta_write(
            mode=mode,
            table_path=table_path,
            df=df,
            storage_options=storage_options,
            partition_key=partition_key,
            unique_key=unique_key,
        )
