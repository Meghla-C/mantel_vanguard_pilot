import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from rules_engine.AbstractRuleValidator import AbstractRuleValidator
from helpers import assert_dataframes_are_equal
from pyspark.sql import DataFrame
from datetime import datetime
from freezegun import freeze_time
from pyspark.sql import Row
from core_functions.config_models import RuleConfig


# rule validator
class RuleValidator(AbstractRuleValidator):
    def execute_rule(self, subset_df: DataFrame) -> DataFrame:
        pass

    def execute_engine(self, subset_df: DataFrame) -> DataFrame:
        pass


@pytest.fixture
def setup_mocks():
    # Mock the data writer factory and the write_to_target_table method
    with patch(
        "writers.DataWriterFactory.DataWriterFactory.get_datawriter"
    ) as mock_get_writer:
        mock_data_writer = MagicMock()
        mock_get_writer.return_value = mock_data_writer

        yield mock_get_writer, mock_data_writer


@freeze_time("2025-10-01T00:00:00Z")
@pytest.mark.parametrize(
    "start_timestamp, end_timestamp, spark_table, table_ddl, get_start_timestamp_result, rule_config, expected_result",
    [
        (
            None,
            None,
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
            "col1 string, md_created_datetime timestamp",
            "2024-01-13 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [["val2", "2024-02-01 00:00:00"]],
        ),
        (
            None,
            None,
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
            "col1 string,md_created_datetime timestamp",
            "2024-03-11 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [],
        ),
        (
            "2024-01-13 00:00:00",
            None,
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
            "col1 string, md_created_datetime timestamp",
            None,
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [["val2", "2024-02-01 00:00:00"]],
        ),
        (
            None,
            None,
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
            "col1 string, md_created_datetime timestamp",
            "1900-01-01 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
        ),
        (
            "1900-01-01 00:00:00",
            None,
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
            "col1 string, md_created_datetime timestamp",
            None,
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
        ),
        (
            "2024-07-01 00:00:00",
            None,
            [["val1", "2024-01-01 00:00:00"], ["val2", "2024-02-01 00:00:00"]],
            "col1 string, md_created_datetime timestamp",
            None,
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [],
        ),
        (
            "2024-01-13 00:00:00",
            "2024-05-11 00:00:00",
            [
                ["val1", "2024-01-01 00:00:00"],
                ["val2", "2024-02-01 00:00:00"],
                ["val3", "2024-06-01 00:00:00"],
            ],
            "col1 string, md_created_datetime timestamp",
            None,
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [["val2", "2024-02-01 00:00:00"]],
        ),
        (
            "2024-01-13 00:00:00",
            "2024-01-14 00:00:00",
            [
                ["val1", "2024-01-01 00:00:00"],
                ["val2", "2024-02-01 00:00:00"],
                ["val3", "2024-06-01 00:00:00"],
            ],
            "col1 string, md_created_datetime timestamp",
            None,
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [],
        ),
        (
            None,
            None,
            [
                [1, "2024-01-01 10:00:00"],
                [2, "2024-01-02 12:00:00"],
                [3, "2024-01-03 14:00:00"],
                [4, "2024-01-04 16:00:00"],
                [5, "2024-01-05 18:00:00"],
                [6, "2024-01-06 18:00:00"],
                [3, "2024-01-07 18:00:00"],
                [8, "2024-01-08 18:00:00"],
                [9, "2024-01-09 18:00:00"],
            ],
            "col1 int, md_created_datetime timestamp",
            "2024-01-07 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [
                [3, "2024-01-07 18:00:00"],
                [8, "2024-01-08 18:00:00"],
                [9, "2024-01-09 18:00:00"],
            ],
        ),
        (
            None,
            None,
            [
                [1, "2024-01-01 10:00:00"],
                [2, "2024-01-02 12:00:00"],
                [3, "2024-01-03 14:00:00"],
                [4, "2024-01-04 16:00:00"],
                [5, "2024-01-05 18:00:00"],
                [6, "2024-01-06 18:00:00"],
                [3, "2024-01-07 18:00:00"],
                [8, "2024-01-08 18:00:00"],
                [9, "2024-01-09 18:00:00"],
            ],
            "col1 int, md_created_datetime timestamp",
            "2024-01-07 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "custom_columns_source_data",
                "subset_column_list": ["col1"],
            },
            [
                [3, "2024-01-03 14:00:00"],
                [4, "2024-01-04 16:00:00"],
                [5, "2024-01-05 18:00:00"],
                [6, "2024-01-06 18:00:00"],
                [3, "2024-01-07 18:00:00"],
                [8, "2024-01-08 18:00:00"],
                [9, "2024-01-09 18:00:00"],
            ],
        ),
        (
            None,
            None,
            [
                [1, "2024-01-01 10:00:00"],
                [2, "2024-01-02 12:00:00"],
                [3, "2024-01-03 14:00:00"],
                [4, "2024-01-04 16:00:00"],
                [5, "2024-01-05 18:00:00"],
                [6, "2024-01-06 18:00:00"],
                [3, "2024-01-07 18:00:00"],
                [4, "2024-01-08 18:00:00"],
            ],
            "col1 int, md_created_datetime timestamp",
            "2024-01-07 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [[3, "2024-01-07 18:00:00"], [4, "2024-01-08 18:00:00"]],
        ),
        (
            None,
            None,
            [
                [1, "2024-01-01 10:00:00"],
                [2, "2024-01-02 12:00:00"],
                [3, "2024-01-03 14:00:00"],
                [4, "2024-01-04 16:00:00"],
                [5, "2024-01-05 18:00:00"],
                [6, "2024-01-06 18:00:00"],
                [3, "2024-01-07 18:00:00"],
                [4, "2024-01-08 18:00:00"],
            ],
            "col1 int, md_created_datetime timestamp",
            "2024-01-07 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "custom_columns_source_data",
                "subset_column_list": ["col1"],
            },
            [
                [3, "2024-01-03 14:00:00"],
                [4, "2024-01-04 16:00:00"],
                [3, "2024-01-07 18:00:00"],
                [4, "2024-01-08 18:00:00"],
            ],
        ),
        (
            None,
            None,
            [
                [1, "2024-01-01 10:00:00", "A"],
                [2, "2024-01-02 12:00:00", "B"],
                [1, "2024-01-03 14:00:00", "B"],
                [3, "2024-01-04 16:00:00", "C"],
                [4, "2024-01-05 18:00:00", "B"],
            ],
            "col1 int, md_created_datetime timestamp, col3 string",
            "2024-01-03 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "watermark_results_table",
            },
            [
                [1, "2024-01-03 14:00:00", "B"],
                [3, "2024-01-04 16:00:00", "C"],
                [4, "2024-01-05 18:00:00", "B"],
            ],
        ),
        (
            None,
            None,
            [
                [1, "2024-01-01 10:00:00", "A"],
                [2, "2024-01-02 12:00:00", "B"],
                [1, "2024-01-03 14:00:00", "B"],
                [3, "2024-01-04 16:00:00", "C"],
                [4, "2024-01-05 18:00:00", "B"],
            ],
            "col1 int, md_created_datetime timestamp, col3 string",
            "2024-01-03 00:00:00",
            {
                "validation_rule": "test",
                "subset_method": "custom_columns_source_data",
                "subset_column_list": ["col1", "col3"],
            },
            [
                [2, "2024-01-02 12:00:00", "B"],
                [1, "2024-01-03 14:00:00", "B"],
                [3, "2024-01-04 16:00:00", "C"],
                [4, "2024-01-05 18:00:00", "B"],
            ],
        ),
    ],
)
def test_subset_data_from_source(
    start_timestamp,
    end_timestamp,
    spark_table,
    table_ddl,
    get_start_timestamp_result,
    rule_config,
    expected_result,
    databricks_spark,
    dbutils_mock,
):

    spark_table_typed = [
        [row[0], datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")]
        + ([row[2]] if len(row) > 2 else [])
        for row in spark_table
    ]
    expected_result_typed = [
        [row[0], datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")]
        + ([row[2]] if len(row) > 2 else [])
        for row in expected_result
    ]

    spark = databricks_spark
    source_df = spark.createDataFrame(spark_table_typed, table_ddl)
    expected_df = spark.createDataFrame(expected_result_typed, table_ddl)
    spark.table = MagicMock(return_value=source_df)

    spark.catalog.tableExists = MagicMock(return_value=source_df)

    base_config = {
        "unique_key": "pk",
        "results_table_name": "schema.results_table",
        "source_table_name": "schema.source_df",
        "validator": "generic",
    }

    rule_config = {
        **base_config,
        **rule_config,
    }

    rule_validator = RuleValidator(
        RuleConfig(**rule_config), databricks_spark, dbutils_mock
    )

    rule_validator.get_start_timestamp = MagicMock(
        return_value=get_start_timestamp_result
    )

    return_df = rule_validator.subset_data_from_source(start_timestamp, end_timestamp)
    assert_dataframes_are_equal(return_df, expected_df)


@pytest.mark.parametrize(
    "results_table, table_ddl, table_exists, source_table_name, expected_start_timestamp",
    [
        (
            [
                ["name2", "rule1", "2024-01-01 00:00:00"],
                ["name2", "rule1", "2024-02-01 00:00:00"],
            ],
            "source_table_name string, rule_name string, md_created_datetime timestamp",
            True,
            "name1",
            "1900-01-01 00:00:00",
        ),
        (
            [],
            "source_table_name string, rule_name string, md_created_datetime timestamp",
            False,
            "name1",
            "1900-01-01 00:00:00",
        ),
    ],
)
def test_get_start_timestamp(
    results_table,
    table_ddl,
    table_exists,
    source_table_name,
    expected_start_timestamp,
    databricks_spark,
    dbutils_mock,
):

    results_table_typed = [
        [row[0], row[1], datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")]
        for row in results_table
    ]

    spark = databricks_spark
    results_df = spark.createDataFrame(results_table_typed, table_ddl)
    spark.table = MagicMock(return_value=results_df)
    spark.catalog.tableExists = MagicMock(return_value=table_exists)

    # mock the rule_config
    rule_config = {
        "validation_rule": "test",
        "results_table_name": "schema.results_table",
        "rule_name": "rule1",
        "source_table_name": "schema." + source_table_name,
        "validator": "generic",
        "unique_key": "pk",
        "subset_method": "watermark_results_table",
    }

    rule_validator = RuleValidator(
        RuleConfig(**rule_config), databricks_spark, dbutils_mock
    )
    start_timestamp = rule_validator.get_start_timestamp()

    assert start_timestamp == expected_start_timestamp


@freeze_time("2024-01-01T00:00:00Z")
@pytest.mark.parametrize(
    "results_table, table_ddl, expected_table",
    [
        (
            [["pk", "pkcol", "rule", 1, 1, "INVALID"]],
            "unique_key string, unique_key_column string, rule_name string, element_count int, unexpected_count int, validation_status string",
            [
                [
                    "pk",
                    "pkcol",
                    "rule",
                    1,
                    1,
                    "source_table",
                    "INVALID",
                    "pk_val_result_id",
                    "metadata",
                    "source_name",
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:00",
                    None,
                    "I",
                ]
            ],
        )
    ],
)
def test_update_results_table(
    results_table,
    table_ddl,
    expected_table,
    databricks_spark,
    dbutils_mock,
    setup_mocks,
):
    expected_table_typed = [
        [
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            datetime.strptime(row[10], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(row[11], "%Y-%m-%d %H:%M:%S"),
            row[12],
            row[13],
        ]
        for row in expected_table
    ]
    schema = StructType(
        [
            StructField("unique_key", StringType(), True),
            StructField("unique_key_column", StringType(), True),
            StructField("rule_name", StringType(), True),
            StructField("element_count", IntegerType(), True),
            StructField("unexpected_count", IntegerType(), True),
            StructField("source_table_name", StringType(), False),
            StructField("md_validation_status", StringType(), True),
            StructField("validation_result_id", StringType(), False),
            StructField("md_metadata", StringType(), True),
            StructField("md_source_name", StringType(), False),
            StructField("md_created_datetime", TimestampType(), False),
            StructField("md_updated_datetime", TimestampType(), False),
            StructField("md_deleted_datetime", TimestampType(), True),
            StructField("md_change_status", StringType(), False),
        ]
    )

    spark = databricks_spark
    expected_df = spark.createDataFrame(data=expected_table_typed, schema=schema)
    results_df = spark.createDataFrame(results_table, table_ddl)

    # mock the rule_config
    rule_config = {
        "validation_rule": "test",
        "results_table_name": "schema.results_table",
        "source_table_name": "schema.source_table",
        "validator": "generic",
        "unique_key": "pk",
        "subset_method": "watermark_results_table",
    }

    rule_validator = RuleValidator(
        RuleConfig(**rule_config), databricks_spark, dbutils_mock
    )
    updates_df = rule_validator.update_results_table(results_df)

    _, mock_data_writer = setup_mocks

    mock_data_writer.write_to_target_table.assert_called_once_with(updates_df)
    assert updates_df.schema == expected_df.schema
