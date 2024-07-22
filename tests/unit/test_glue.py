from unittest.mock import call

import pytest

from dbt.adapters.base.column import Column
from dbt.adapters.duckdb.plugins.glue import create_or_update_table


class TestGlue:
    @pytest.fixture
    def columns(self):
        return [
            Column(column="bigint", dtype="BIGINT"),
            Column(column="int8", dtype="INT8"),
            Column(column="long", dtype="LONG"),
            Column(column="boolean", dtype="BOOLEAN"),
            Column(column="logical", dtype="LOGICAL"),
            Column(column="blob", dtype="BLOB"),
            Column(column="bytea", dtype="BYTEA"),
            Column(column="varbinary", dtype="VARBINARY"),
            Column(column="date", dtype="DATE"),
            Column(column="double", dtype="DOUBLE"),
            Column(column="float8", dtype="FLOAT8"),
            Column(column="numeric", dtype="NUMERIC"),
            Column(column="decimal", dtype="DECIMAL"),
            Column(column="decimal_3_2", dtype="DECIMAL"),
            Column(column="integer", dtype="INTEGER"),
            Column(column="int4", dtype="INT4"),
            Column(column="int", dtype="INT"),
            Column(column="signed", dtype="SIGNED"),
            Column(column="real", dtype="REAL"),
            Column(column="float4", dtype="FLOAT4"),
            Column(column="float", dtype="FLOAT"),
            Column(column="smallint", dtype="SMALLINT"),
            Column(column="int2", dtype="INT2"),
            Column(column="short", dtype="SHORT"),
            Column(column="timestamp", dtype="TIMESTAMP"),
            Column(column="datetime", dtype="DATETIME"),
            Column(column="timestamptz", dtype="TIMESTAMPTZ"),
            Column(column="tinyint", dtype="TINYINT"),
            Column(column="int1", dtype="INT1"),
            Column(column="uinteger", dtype="UINTEGER"),
            Column(column="usmallint", dtype="USMALLINT"),
            Column(column="utinyint", dtype="UTINYINT"),
            Column(column="uuid", dtype="UUID"),
            Column(column="varchar", dtype="VARCHAR"),
            Column(column="char", dtype="CHAR"),
            Column(column="bpchar", dtype="BPCHAR"),
            Column(column="text", dtype="TEXT"),
            Column(column="string", dtype="STRING"),
        ]

    def test_create_glue_table(self, mocker, columns):
        client = mocker.Mock()
        client.get_table.return_value = None
        create_or_update_table(
            client,
            database="test",
            table="test",
            column_list=columns,
            s3_path="s3://test/aaa.parquet",
            file_format="parquet",
            delimiter=",",
        )

        client.assert_has_calls(
            [
                call.get_table(DatabaseName="test", Name="test"),
                call.create_table(
                    DatabaseName="test",
                    TableInput={
                        "Name": "test",
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {"classification": "parquet"},
                        "StorageDescriptor": {
                            "Columns": [
                                {"Name": "bigint", "Type": "bigint"},
                                {"Name": "int8", "Type": "bigint"},
                                {"Name": "long", "Type": "bigint"},
                                {"Name": "boolean", "Type": "boolean"},
                                {"Name": "logical", "Type": "boolean"},
                                {"Name": "blob", "Type": "binary"},
                                {"Name": "bytea", "Type": "binary"},
                                {"Name": "varbinary", "Type": "binary"},
                                {"Name": "date", "Type": "date"},
                                {"Name": "double", "Type": "double"},
                                {"Name": "float8", "Type": "double"},
                                {"Name": "numeric", "Type": "double"},
                                {"Name": "decimal", "Type": "double"},
                                {"Name": "decimal_3_2", "Type": "double"},
                                {"Name": "integer", "Type": "int"},
                                {"Name": "int4", "Type": "int"},
                                {"Name": "int", "Type": "int"},
                                {"Name": "signed", "Type": "int"},
                                {"Name": "real", "Type": "float"},
                                {"Name": "float4", "Type": "float"},
                                {"Name": "float", "Type": "float"},
                                {"Name": "smallint", "Type": "smallint"},
                                {"Name": "int2", "Type": "smallint"},
                                {"Name": "short", "Type": "smallint"},
                                {"Name": "timestamp", "Type": "timestamp"},
                                {"Name": "datetime", "Type": "timestamp"},
                                {"Name": "timestamptz", "Type": "timestamp"},
                                {"Name": "tinyint", "Type": "tinyint"},
                                {"Name": "int1", "Type": "tinyint"},
                                {"Name": "uinteger", "Type": "bigint"},
                                {"Name": "usmallint", "Type": "int"},
                                {"Name": "utinyint", "Type": "smallint"},
                                {"Name": "uuid", "Type": "string"},
                                {"Name": "varchar", "Type": "string"},
                                {"Name": "char", "Type": "string"},
                                {"Name": "bpchar", "Type": "string"},
                                {"Name": "text", "Type": "string"},
                                {"Name": "string", "Type": "string"},
                            ],
                            "Location": "s3://test",
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": -1,
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "Parameters": {"serialization.format": "1"},
                            },
                            "BucketColumns": [],
                            "StoredAsSubDirectories": False,
                            "SortColumns": [],
                        },
                    },
                ),
            ]
        )

    def test_update_glue_table(self, mocker, columns):
        client = mocker.Mock()
        client.get_table.return_value = {
            "Table": {
                "Name": "test",
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet"},
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "n_legs", "Type": "bigint"},
                    ],
                    "Location": "test",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "Compressed": False,
                    "NumberOfBuckets": -1,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                    "BucketColumns": [],
                    "StoredAsSubDirectories": False,
                    "SortColumns": [],
                },
            },
        }
        create_or_update_table(
            client,
            database="test",
            table="test",
            column_list=columns,
            s3_path="s3://test/aaa.parquet",
            file_format="parquet",
            delimiter=",",
        )
        client.assert_has_calls(
            [
                call.get_table(DatabaseName="test", Name="test"),
                call.update_table(
                    DatabaseName="test",
                    TableInput={
                        "Name": "test",
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {"classification": "parquet"},
                        "StorageDescriptor": {
                            "Columns": [
                                {"Name": "bigint", "Type": "bigint"},
                                {"Name": "int8", "Type": "bigint"},
                                {"Name": "long", "Type": "bigint"},
                                {"Name": "boolean", "Type": "boolean"},
                                {"Name": "logical", "Type": "boolean"},
                                {"Name": "blob", "Type": "binary"},
                                {"Name": "bytea", "Type": "binary"},
                                {"Name": "varbinary", "Type": "binary"},
                                {"Name": "date", "Type": "date"},
                                {"Name": "double", "Type": "double"},
                                {"Name": "float8", "Type": "double"},
                                {"Name": "numeric", "Type": "double"},
                                {"Name": "decimal", "Type": "double"},
                                {"Name": "decimal_3_2", "Type": "double"},
                                {"Name": "integer", "Type": "int"},
                                {"Name": "int4", "Type": "int"},
                                {"Name": "int", "Type": "int"},
                                {"Name": "signed", "Type": "int"},
                                {"Name": "real", "Type": "float"},
                                {"Name": "float4", "Type": "float"},
                                {"Name": "float", "Type": "float"},
                                {"Name": "smallint", "Type": "smallint"},
                                {"Name": "int2", "Type": "smallint"},
                                {"Name": "short", "Type": "smallint"},
                                {"Name": "timestamp", "Type": "timestamp"},
                                {"Name": "datetime", "Type": "timestamp"},
                                {"Name": "timestamptz", "Type": "timestamp"},
                                {"Name": "tinyint", "Type": "tinyint"},
                                {"Name": "int1", "Type": "tinyint"},
                                {"Name": "uinteger", "Type": "bigint"},
                                {"Name": "usmallint", "Type": "int"},
                                {"Name": "utinyint", "Type": "smallint"},
                                {"Name": "uuid", "Type": "string"},
                                {"Name": "varchar", "Type": "string"},
                                {"Name": "char", "Type": "string"},
                                {"Name": "bpchar", "Type": "string"},
                                {"Name": "text", "Type": "string"},
                                {"Name": "string", "Type": "string"},
                            ],
                            "Location": "s3://test",
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "Compressed": False,
                            "NumberOfBuckets": -1,
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "Parameters": {"serialization.format": "1"},
                            },
                            "BucketColumns": [],
                            "StoredAsSubDirectories": False,
                            "SortColumns": [],
                        },
                    },
                ),
            ]
        )

    def test_without_update_glue_table(self, mocker, columns):
        client = mocker.Mock()
        client.get_table.return_value = {
            "Table": {
                "Name": "test",
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet",
                },
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "bigint", "Type": "bigint"},
                        {"Name": "int8", "Type": "bigint"},
                        {"Name": "long", "Type": "bigint"},
                        {"Name": "boolean", "Type": "boolean"},
                        {"Name": "logical", "Type": "boolean"},
                        {"Name": "blob", "Type": "binary"},
                        {"Name": "bytea", "Type": "binary"},
                        {"Name": "varbinary", "Type": "binary"},
                        {"Name": "date", "Type": "date"},
                        {"Name": "double", "Type": "double"},
                        {"Name": "float8", "Type": "double"},
                        {"Name": "numeric", "Type": "double"},
                        {"Name": "decimal", "Type": "double"},
                        {"Name": "decimal_3_2", "Type": "double"},
                        {"Name": "integer", "Type": "int"},
                        {"Name": "int4", "Type": "int"},
                        {"Name": "int", "Type": "int"},
                        {"Name": "signed", "Type": "int"},
                        {"Name": "real", "Type": "float"},
                        {"Name": "float4", "Type": "float"},
                        {"Name": "float", "Type": "float"},
                        {"Name": "smallint", "Type": "smallint"},
                        {"Name": "int2", "Type": "smallint"},
                        {"Name": "short", "Type": "smallint"},
                        {"Name": "timestamp", "Type": "timestamp"},
                        {"Name": "datetime", "Type": "timestamp"},
                        {"Name": "timestamptz", "Type": "timestamp"},
                        {"Name": "tinyint", "Type": "tinyint"},
                        {"Name": "int1", "Type": "tinyint"},
                        {"Name": "uinteger", "Type": "bigint"},
                        {"Name": "usmallint", "Type": "int"},
                        {"Name": "utinyint", "Type": "smallint"},
                        {"Name": "uuid", "Type": "string"},
                        {"Name": "varchar", "Type": "string"},
                        {"Name": "char", "Type": "string"},
                        {"Name": "bpchar", "Type": "string"},
                        {"Name": "text", "Type": "string"},
                        {"Name": "string", "Type": "string"},
                    ],
                    "Location": "test",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "Compressed": True,
                    "NumberOfBuckets": -1,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                    "BucketColumns": [],
                    "StoredAsSubDirectories": False,
                    "SortColumns": [],
                },
            },
        }
        create_or_update_table(
            client,
            database="test",
            table="test",
            column_list=columns,
            s3_path="test",
            file_format="parquet",
            delimiter=",",
        )
        assert len(client.mock_calls) == 1
        client.assert_has_calls([call.get_table(DatabaseName="test", Name="test")])
