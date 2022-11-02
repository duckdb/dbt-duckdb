from unittest.mock import call

import pytest
from dbt.adapters.duckdb.glue import create_or_update_table
from dbt.tests.util import get_connection, relation_from_name


@pytest.fixture(scope="class")
def columns(project):
    table_name = "column_test"
    project.run_sql(
        f"""
        create or replace table {project.database}.{project.test_schema}.{table_name}(
            bigint BIGINT,
            int8 INT8,
            long LONG,
            boolean BOOLEAN,
            logical LOGICAL,
            blob BLOB,
            bytea BYTEA,
            varbinary VARBINARY,
            date DATE,
            double DOUBLE,
            float8 FLOAT8,
            numeric NUMERIC,
            decimal DECIMAL,
            decimal_3_2 DECIMAL,
            integer INTEGER,
            int4 INT4,
            int INT,
            signed SIGNED,
            real REAL,
            float4 FLOAT4,
            float FLOAT,
            smallint SMALLINT,
            int2 INT2,
            short SHORT,
            timestamp TIMESTAMP,
            datetime DATETIME,
            timestamptz TIMESTAMPTZ,
            tinyint TINYINT,
            int1 INT1,
            uinteger UINTEGER,
            usmallint USMALLINT,
            utinyint UTINYINT,
            uuid UUID,
            varchar VARCHAR,
            char CHAR,
            bpchar BPCHAR,
            text TEXT,
            string STRING
        );
        """
    )
    adapter = project.adapter
    relation = relation_from_name(adapter, table_name)
    with get_connection(adapter):
        columns = adapter.get_columns_in_relation(relation)
    return columns


def test_create_glue_table(mocker, columns):
    boto3 = mocker.patch("dbt.adapters.duckdb.glue.boto3.client")
    boto3.return_value.get_table.return_value = None
    create_or_update_table(
        database="test",
        table="test",
        column_list=columns,
        s3_path="test",
        file_format="parquet",
    )
    boto3.assert_has_calls(
        [
            call("glue"),
            call().get_table(DatabaseName="test", Name="test"),
            call().create_table(
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
            ),
        ]
    )


def test_update_glue_table(mocker, columns):
    boto3 = mocker.patch("dbt.adapters.duckdb.glue.boto3.client")
    boto3.return_value.get_table.return_value = {
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
        database="test",
        table="test",
        column_list=columns,
        s3_path="test",
        file_format="parquet",
    )
    boto3.assert_has_calls(
        [
            call("glue"),
            call().get_table(DatabaseName="test", Name="test"),
            call().update_table(
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
            ),
        ]
    )


def test_without_update_glue_table(mocker, columns):
    boto3 = mocker.patch("dbt.adapters.duckdb.glue.boto3.client")
    boto3.return_value.get_table.return_value = {
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
        database="test",
        table="test",
        column_list=columns,
        s3_path="test",
        file_format="parquet",
    )
    assert len(boto3.mock_calls) == 2
    boto3.has_calls([call("glue"), call().get_table(DatabaseName="test", Name="test")])
