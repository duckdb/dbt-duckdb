import os
import pyarrow.compute as pc
from typing import Any
from typing import Dict

from deltalake import DeltaTable, write_deltalake

from . import BasePlugin
from ..utils import SourceConfig, TargetConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        pass

    def configure_cursor(self, cursor):
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception(
                "'delta_table_path' is a required argument for the delta table!"
            )

        table_path = source_config["delta_table_path"]
        storage_options = source_config.get("storage_options", {})

        dt = DeltaTable(table_path, storage_options=storage_options)

        # delta attributes
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)

        if as_of_version:
            dt.load_version(as_of_version)

        if as_of_datetime:
            dt.load_with_datetime(as_of_datetime)

        df = dt.to_pyarrow_dataset()

        return df

    def default_materialization(self):
        return "view"

    def store(self, target_config: TargetConfig, df=None):
        mode = target_config.config.get("mode", "overwrite")
        table_path = target_config.location.path
        storage_options = target_config.config.get("storage_options", {})

        if mode == "overwrite_partition":
            partition_key = target_config.config.get("partition_key", None)
            if not partition_key:
                raise Exception(
                    "'partition_key' has to be defined when mode 'overwrite_partition'!"
                )

            if isinstance(partition_key, str):
                partition_key = [partition_key]

            partition_dict = []
            for each_key in partition_key:
                unique_key_array = pc.unique(df[each_key])

                if len(unique_key_array) == 1:
                    partition_dict.append((each_key, str(unique_key_array[0])))
                else:
                    raise Exception(
                        f"'{each_key}' column has not one unique value, values are: {str(unique_key_array)}"
                    )
            create_insert_partition(table_path, df, partition_dict, storage_options)
        elif mode == "merge":
            # very slow -> https://github.com/delta-io/delta-rs/issues/1846
            unique_key = target_config.config.get("unique_key", None)
            if not unique_key:
                raise Exception("'unique_key' has to be defined when mode 'merge'!")
            if isinstance(unique_key, str):
                unique_key = [unique_key]

            predicate_stm = " and ".join(
                [
                    f'source."{each_unique_key}" = target."{each_unique_key}"'
                    for each_unique_key in unique_key
                ]
            )

            try:
                target_dt = DeltaTable(table_path, storage_options=storage_options)
            except Exception:
                # TODO handle this better
                write_deltalake(
                    table_or_uri=table_path, data=df, storage_options=storage_options
                )

            target_dt = DeltaTable(table_path, storage_options=storage_options)
            # TODO there is a problem if the column name is uppercase
            target_dt.merge(
                source=df,
                predicate=predicate_stm,
                source_alias="source",
                target_alias="target",
            ).when_not_matched_insert_all().execute()
        else:
            write_deltalake(
                table_or_uri=table_path,
                data=df,
                mode=mode,
                storage_options=storage_options,
            )


def table_exists(table_path, storage_options):
    # this is bad, i have to find the way to see if there is table behind path
    try:
        DeltaTable(table_path, storage_options=storage_options)
    except Exception:
        return False
    return True


## TODO
# add partition writing
# add optimization, vacumm options to automatically run before each run ?
# can deltars optimize if the data is bigger then memory?


def create_insert_partition(table_path, data, partitions, storage_options):
    """create a new delta table on the path or overwrite existing partition"""

    if table_exists(table_path, storage_options):
        partition_expr = [
            (partition_name, "=", partition_value)
            for (partition_name, partition_value) in partitions
        ]
        print(
            f"Overwriting delta table under: {table_path} \nwith partition expr: {partition_expr}"
        )
        write_deltalake(
            table_path, data, partition_filters=partition_expr, mode="overwrite"
        )
    else:
        partitions = [
            partition_name for (partition_name, partition_value) in partitions
        ]
        print(
            f"Creating delta table under: {table_path} \nwith partitions: {partitions}"
        )
        write_deltalake(table_path, data, partition_by=partitions)


# Future
# TODO add databricks catalog
