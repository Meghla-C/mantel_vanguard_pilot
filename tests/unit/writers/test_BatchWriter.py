import pytest
from pyspark.sql import DataFrame
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from writers.BatchWriter import BatchWriter
from pyspark.sql import Row
from helpers import assert_dataframes_are_equal
from core_functions.config_models import EntityConfig


def test_invalid_target_table_name(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "target": {
            "writer_type": "batch",
            "writer_mode": "append",
            "table_name": "test_schema",
        },
    }
    with pytest.raises(
        ValueError,
        match="target_table_name with its schema name is required",
    ):

        BatchWriter(EntityConfig(**entity_config), spark_mock, dbutils_mock)


def test_invalid_meta_table_name(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "target": {
            "writer_type": "batch",
            "writer_mode": "append",
            "table_name": "test_schema.test_table",
            "enable_file_watermarking": True,
            "meta_table_name": "test_schema",
        },
    }
    with pytest.raises(
        ValueError,
        match="meta_table_name with its schema name is required when enable_file_watermarking is True",
    ):
        BatchWriter(EntityConfig(**entity_config), spark_mock, dbutils_mock)


def test_overwrite_batch_writer(spark_mock, dbutils_mock):
    mock_source_df = MagicMock()

    entity_config = {
        "entity_name": "test_entity",
        "target": {
            "writer_type": "batch",
            "table_name": "test_schema.test_table",
            "writer_mode": "overwrite",
        },
    }

    bw = BatchWriter(EntityConfig(**entity_config), spark_mock, dbutils_mock)
    bw.write_to_target_table(mock_source_df)

    mock_source_df.write.format.assert_called_once_with("delta")
    mock_source_df.write.format().mode.assert_called_once_with("overwrite")
    mock_source_df.write.format().mode().options.assert_called_once_with()
    mock_source_df.write.format().mode().options().saveAsTable.assert_called_once_with(
        "test_schema.test_table"
    )


def test_append_batch_writer(spark_mock, dbutils_mock):
    mock_source_df = MagicMock()

    entity_config = {
        "entity_name": "test_entity",
        "target": {
            "writer_type": "batch",
            "table_name": "test_schema.test_table",
            "writer_mode": "append",
        },
    }

    bw = BatchWriter(EntityConfig(**entity_config), spark_mock, dbutils_mock)
    bw.write_to_target_table(mock_source_df)

    mock_source_df.write.format.assert_called_once_with("delta")
    mock_source_df.write.format().mode.assert_called_once_with("append")
    mock_source_df.write.format().mode().options.assert_called_once_with()
    mock_source_df.write.format().mode().options().saveAsTable.assert_called_once_with(
        "test_schema.test_table"
    )


@pytest.mark.parametrize(
    "source_df, target_df, entity_config, expected_df",
    [
        (
            [Row(name="Diana", age=40)],
            [
                Row(name="Alice", age=25),
                Row(name="Bob", age=30),
                Row(name="Charlie", age=35),
            ],
            {
                "target": {
                    "writer_type": "batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "delete_condition": "age > 30",
                    "writer_mode": "overwrite",
                }
            },
            [Row(name="Diana", age=40)],
        ),
        (
            [Row(name="Diana", age=40)],
            [
                Row(name="Alice", age=25),
                Row(name="Bob", age=30),
                Row(name="Charlie", age=35),
            ],
            {
                "target": {
                    "writer_type": "batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "delete_condition": "name = 'Alice'",
                    "writer_mode": "append",
                }
            },
            [
                Row(name="Bob", age=30),
                Row(name="Charlie", age=35),
                Row(name="Diana", age=40),
            ],
        ),
        (
            [Row(name="Diana", age=40)],
            [
                Row(name="Alice", age=25),
                Row(name="Bob", age=30),
                Row(name="Charlie", age=35),
            ],
            {
                "target": {
                    "writer_type": "batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "delete_condition": None,
                    "writer_mode": "append",
                }
            },
            [
                Row(name="Alice", age=25),
                Row(name="Bob", age=30),
                Row(name="Charlie", age=35),
                Row(name="Diana", age=40),
            ],
        ),
    ],
)
def test_table_batch_writer(
    source_df,
    target_df,
    entity_config,
    expected_df,
    databricks_spark,
    dbutils_mock,
):
    spark = databricks_spark

    source_df = spark.createDataFrame(source_df)

    target_df = spark.createDataFrame(target_df)

    spark.sql("DROP TABLE IF EXISTS meta.dummy_table_for_unit_test")
    target_df.write.mode("overwrite").saveAsTable("meta.dummy_table_for_unit_test")

    bw = BatchWriter(EntityConfig(**entity_config), spark, dbutils_mock)
    bw.write_to_target_table(source_df, entity_config, spark)
    result_df = spark.sql("SELECT * FROM meta.dummy_table_for_unit_test")

    expected_df = spark.createDataFrame(expected_df)

    assert_dataframes_are_equal(result_df, expected_df)

    spark.sql("DROP TABLE IF EXISTS meta.dummy_table_for_unit_test")
