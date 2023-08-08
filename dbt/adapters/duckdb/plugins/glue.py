from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

import boto3
from mypy_boto3_glue import GlueClient
from mypy_boto3_glue.type_defs import ColumnTypeDef
from mypy_boto3_glue.type_defs import GetTableResponseTypeDef
from mypy_boto3_glue.type_defs import SerDeInfoTypeDef
from mypy_boto3_glue.type_defs import StorageDescriptorTypeDef
from mypy_boto3_glue.type_defs import TableInputTypeDef

from . import BasePlugin
from ..utils import TargetConfig
from dbt.adapters.base.column import Column


class UnsupportedFormatType(Exception):
    """UnsupportedFormatType exception."""


class UnsupportedType(Exception):
    """UnsupportedType exception."""


class UndetectedType(Exception):
    """UndetectedType exception."""


def _dbt2glue(dtype: str, ignore_null: bool = False) -> str:  # pragma: no cover
    """DuckDB to Glue data types conversion."""
    data_type = dtype.split("(")[0]
    if data_type.lower() in ["int1", "tinyint"]:
        return "tinyint"
    if data_type.lower() in ["int2", "smallint", "short", "utinyint"]:
        return "smallint"
    if data_type.lower() in ["int4", "int", "integer", "signed", "usmallint"]:
        return "int"
    if data_type.lower() in ["int8", "long", "bigint", "signed", "uinteger"]:
        return "bigint"
    if data_type.lower() in ["hugeint", "ubigint"]:
        raise UnsupportedType(
            "There is no support for hugeint or ubigint, please consider bigint or uinteger."
        )
    if data_type.lower() in ["float4", "float", "real"]:
        return "float"
    if data_type.lower() in ["float8", "numeric", "decimal", "double"]:
        return "double"
    if data_type.lower() in ["boolean", "bool", "logical"]:
        return "boolean"
    if data_type.lower() in ["varchar", "char", "bpchar", "text", "string", "uuid"]:
        return "string"
    if data_type.lower() in [
        "timestamp",
        "datetime",
        "timestamptz",
        "timestamp with time zone",
    ]:
        return "timestamp"
    if data_type.lower() in ["date"]:
        return "date"
    if data_type.lower() in ["blob", "bytea", "binary", "varbinary"]:
        return "binary"
    if data_type is None:
        if ignore_null:
            return ""
        raise UndetectedType("We can not infer the data type from an entire null object column")
    raise UnsupportedType(f"Unsupported type: {dtype}")


def _get_parquet_table_def(
    table: str, s3_parent: str, columns: Sequence["ColumnTypeDef"]
) -> "TableInputTypeDef":
    """Create table definition for Glue table. See https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html#API_CreateTable_RequestSyntax"""
    table_def = TableInputTypeDef(
        Name=table,
        TableType="EXTERNAL_TABLE",
        Parameters={"classification": "parquet"},
        StorageDescriptor=StorageDescriptorTypeDef(
            Columns=columns,
            Location=s3_parent,
            InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            Compressed=False,
            NumberOfBuckets=-1,
            SerdeInfo=SerDeInfoTypeDef(
                SerializationLibrary="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                Parameters={"serialization.format": "1"},
            ),
            BucketColumns=[],
            StoredAsSubDirectories=False,
            SortColumns=[],
        ),
    )
    return table_def


def _get_csv_table_def(
    table: str, s3_parent: str, columns: Sequence["ColumnTypeDef"], delimiter: str = ","
) -> "TableInputTypeDef":
    """Create table definition for Glue table. See https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html#API_CreateTable_RequestSyntax"""
    table_def = TableInputTypeDef(
        Name=table,
        TableType="EXTERNAL_TABLE",
        Parameters={"classification": "csv"},
        StorageDescriptor=StorageDescriptorTypeDef(
            Columns=columns,
            Location=s3_parent,
            InputFormat="org.apache.hadoop.mapred.TextInputFormat",
            OutputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            Compressed=False,
            NumberOfBuckets=-1,
            SerdeInfo=SerDeInfoTypeDef(
                SerializationLibrary="org.apache.hadoop.hive.serde2.OpenCSVSerde",
                Parameters={"separatorChar": delimiter},
            ),
            BucketColumns=[],
            StoredAsSubDirectories=False,
            SortColumns=[],
        ),
    )
    return table_def


def _convert_columns(column_list: Sequence[Column]) -> Sequence["ColumnTypeDef"]:
    """Convert dbt schema into Glue compliable Schema"""
    column_types = []
    for column in column_list:
        column_types.append(ColumnTypeDef(Name=column.name, Type=_dbt2glue(column.dtype)))
    return column_types


def _create_table(client: "GlueClient", database: str, table_def: "TableInputTypeDef") -> None:
    client.create_table(DatabaseName=database, TableInput=table_def)


def _update_table(client: "GlueClient", database: str, table_def: "TableInputTypeDef") -> None:
    client.update_table(DatabaseName=database, TableInput=table_def)


def _get_table(
    client: "GlueClient", database: str, table: str
) -> Optional["GetTableResponseTypeDef"]:
    try:
        return client.get_table(DatabaseName=database, Name=table)
    except client.exceptions.EntityNotFoundException:  # pragma: no cover
        return None


def _get_column_type_def(
    table_def: "GetTableResponseTypeDef",
) -> Optional[Sequence["ColumnTypeDef"]]:
    """Get columns definition from Glue Table Definition"""
    raw = table_def.get("Table", {}).get("StorageDescriptor", {}).get("Columns")
    if raw:
        converted = []
        for column in raw:
            converted.append(ColumnTypeDef(Name=column["Name"], Type=column["Type"]))
        return converted
    else:
        return None


def _get_table_def(
    table: str,
    s3_path: str,
    columns: Sequence["ColumnTypeDef"],
    file_format: str,
    delimiter: str,
):
    s3_parent = "/".join(s3_path.split("/")[:-1])
    if file_format == "csv":
        table_def = _get_csv_table_def(
            table=table,
            s3_parent=s3_parent,
            columns=columns,
            delimiter=delimiter,
        )
    elif file_format == "parquet":
        table_def = _get_parquet_table_def(table=table, s3_parent=s3_parent, columns=columns)
    else:
        raise UnsupportedFormatType("Format %s is not supported in Glue registrar." % file_format)
    return table_def


def _get_glue_client(settings: Dict[str, Any]) -> "GlueClient":
    if settings:
        return boto3.client(
            "glue",
            aws_access_key_id=settings.get("s3_access_key_id"),
            aws_secret_access_key=settings.get("s3_secret_access_key"),
            aws_session_token=settings.get("s3_session_token"),
            region_name=settings.get("s3_region"),
        )
    else:
        return boto3.client("glue")


def create_or_update_table(
    client: GlueClient,
    database: str,
    table: str,
    column_list: Sequence[Column],
    s3_path: str,
    file_format: str,
    delimiter: str,
) -> None:
    # Existing table in AWS Glue catalog
    glue_table = _get_table(client=client, database=database, table=table)
    columns = _convert_columns(column_list)
    table_def = _get_table_def(
        table=table,
        s3_path=s3_path,
        columns=columns,
        file_format=file_format,
        delimiter=delimiter,
    )
    if glue_table:
        # Existing columns in AWS Glue catalog
        glue_columns = _get_column_type_def(glue_table)
        # Create new version only if columns are changed
        if glue_columns != columns:
            _update_table(client=client, database=database, table_def=table_def)
    else:
        _create_table(client=client, database=database, table_def=table_def)


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self.client = _get_glue_client(config)
        self.database = config.get("glue_database", "default")
        self.delimiter = config.get("delimiter", ",")

    def store(self, target_config: TargetConfig):
        assert target_config.location is not None
        assert target_config.relation.identifier is not None
        table: str = target_config.relation.identifier
        create_or_update_table(
            self.client,
            self.database,
            table,
            target_config.column_list,
            target_config.location.path,
            target_config.location.format,
            self.delimiter,
        )
