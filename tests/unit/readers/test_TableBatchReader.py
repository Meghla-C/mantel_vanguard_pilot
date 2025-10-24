import pytest
from readers.TableBatchReader import TableBatchReader
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql import Row
from datetime import datetime, timedelta
from helpers import assert_dataframes_are_equal
from core_functions.config_models import EntityConfig


def test_invalid_table_name(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {"reader_type": "table_batch", "table_name": "test"},
    }

    expected_error_message = "Full table name with schema is required"

    with pytest.raises(ValueError, match=expected_error_message):
        tr = TableBatchReader(EntityConfig(**entity_config), spark_mock, dbutils_mock)
        tr.read_source_to_dataframe(None)


def test_add_table_metadata(databricks_spark, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "table_batch",
            "table_name": "test_schema.test_table",
        },
    }

    table_reader = TableBatchReader(
        EntityConfig(**entity_config), databricks_spark, dbutils_mock
    )

    mock_data = [
        (
            "value",
            "None",
            "test_entity_pk",
            "metadata",
            "file_name",
            "file_modification_time",
            "created_datetime",
            "updated_datetime",
            "deleted_datetime",
            "change_status",
        )
    ]
    mock_columns = [
        "value",
        "md_validation_status",
        "md_test_entity_pk",
        "md_metadata",
        "md_source_name",
        "md_file_modification_time",
        "md_created_datetime",
        "md_updated_datetime",
        "md_deleted_datetime",
        "md_change_status",
    ]

    df = databricks_spark.createDataFrame(mock_data, mock_columns)

    return_df = table_reader._add_table_metadata(df)

    origin_columns = [
        "md_validation_status_origin",
        "md_test_entity_pk_origin",
        "md_metadata_origin",
        "md_source_name_origin",
        "md_created_datetime_origin",
        "md_updated_datetime_origin",
        "md_deleted_datetime_origin",
        "md_change_status_origin",
    ]

    metadata_columns = [
        "md_validation_status",
        "md_test_entity_pk",
        "md_metadata",
        "md_source_name",
        "md_created_datetime",
        "md_updated_datetime",
        "md_deleted_datetime",
        "md_change_status",
    ]

    for col in origin_columns + metadata_columns:
        assert col in return_df.columns


def test_filter_by_time_range(databricks_spark, spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "table_batch",
            "table_name": "test_schema.test_table",
            "read_options": {},
            "filter_condition": "",
            "datetime_column": "md_updated_datetime",
            "batch_window": 10,
        },
    }

    spark = databricks_spark
    tr = TableBatchReader(EntityConfig(**entity_config), spark_mock, dbutils_mock)
    tr.source_watermark_column = "md_updated_datetime"

    schema = StructType(
        [
            StructField("md_updated_datetime", TimestampType(), True),
            StructField("data", StringType(), True),
        ]
    )

    data = [
        Row(datetime(2024, 10, 3, 12, 0), "row1"),
        Row(datetime(2024, 10, 3, 12, 5), "row2"),
        Row(datetime(2024, 10, 3, 12, 10), "row3"),
        Row(datetime(2024, 10, 3, 12, 15), "row4"),
    ]

    dataframe = spark.createDataFrame(data, schema)
    max_updated_time = datetime(2024, 10, 3, 12, 15)
    time_window = timedelta(minutes=(10 + 0.000001))
    min_time = max_updated_time - time_window

    result_df = tr._filter_by_time_range(dataframe, min_time)

    expected_data = [
        Row(datetime(2024, 10, 3, 12, 5), "row2"),
        Row(datetime(2024, 10, 3, 12, 10), "row3"),
        Row(datetime(2024, 10, 3, 12, 15), "row4"),
    ]

    expected_df = spark.createDataFrame(expected_data, schema)

    print("expected", expected_df.show())
    print("result", result_df.show())

    assert_dataframes_are_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "entity_config, source_df, watermark_table, result_table",
    [
        (  # select query not used and no watermark
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                },
            },
            # source df
            [
                Row(
                    id=1, name="Alice", md_updated_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
            None,  # watermark table
            [
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
        ),
        (  # select query not used and watermark
            # source df
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "watermark_table_name": "meta.dummy_watermark_table_for_unit_test",
                },
            },
            # source df
            [
                Row(
                    id=1, name="Alice", md_updated_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 4),
                ),
            ],
            [
                Row(
                    id=1, name="Alice", md_created_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_created_datetime=datetime(2024, 10, 3, 12, 3)),
            ],  # watermark table
            [
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 4),
                ),
            ],
        ),
        (  # select query not used and time delta
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "batch_window": 1,
                },
            },
            # source df
            [
                Row(
                    id=1, name="Alice", md_updated_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
            None,  # watermark table
            [
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
        ),
        (  # select query used and no watermark
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "select_query": "SELECT * FROM meta.dummy_table_for_unit_test WHERE id < 4",
                },
            },
            # source df
            [
                Row(
                    id=1, name="Alice", md_updated_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
                Row(
                    id=4,
                    name="Daniel",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
            None,  # watermark table
            [
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
        ),
        (  # select query used and watermark
            # source df
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "select_query": "SELECT * FROM meta.dummy_table_for_unit_test WHERE id < 4",
                    "watermark_table_name": "meta.dummy_watermark_table_for_unit_test",
                },
            },
            # source df
            [
                Row(
                    id=1, name="Alice", md_updated_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 4),
                ),
                Row(
                    id=4,
                    name="Daniel",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 4),
                ),
            ],
            [
                Row(
                    id=1, name="Alice", md_created_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_created_datetime=datetime(2024, 10, 3, 12, 3)),
            ],  # watermark table
            [
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 4),
                ),
            ],
        ),
        (  # select query used and time delta
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "select_query": "SELECT * FROM meta.dummy_table_for_unit_test WHERE id < 4",
                    "batch_window": 1,
                },
            },
            # source df
            [
                Row(
                    id=1, name="Alice", md_updated_datetime=datetime(2024, 10, 3, 12, 0)
                ),
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
                Row(
                    id=4,
                    name="Daniel",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
            None,  # watermark table
            [
                Row(id=2, name="Bob", md_updated_datetime=datetime(2024, 10, 3, 12, 1)),
                Row(
                    id=3,
                    name="Charlie",
                    md_updated_datetime=datetime(2024, 10, 3, 12, 2),
                ),
            ],
        ),
    ],
)
def test_table_batch_reader(
    entity_config,
    source_df,
    watermark_table,
    result_table,
    databricks_spark,
    dbutils_mock,
):

    spark = databricks_spark

    source_df = spark.createDataFrame(source_df)
    if watermark_table:
        watermark_table = spark.createDataFrame(watermark_table)
        spark.sql("DROP TABLE IF EXISTS meta.dummy_watermark_table_for_unit_test")
        watermark_table.write.mode("overwrite").saveAsTable(
            "meta.dummy_watermark_table_for_unit_test"
        )
    result_table = spark.createDataFrame(result_table)

    spark.sql("DROP TABLE IF EXISTS meta.dummy_table_for_unit_test")
    source_df.write.mode("overwrite").saveAsTable("meta.dummy_table_for_unit_test")

    tbr = TableBatchReader(EntityConfig(**entity_config), spark, dbutils_mock)
    read_df = (
        tbr.read_source_to_dataframe()
        .select("id", "name", "md_updated_datetime_origin")
        .withColumnRenamed("md_updated_datetime_origin", "md_updated_datetime")
    )

    print("result", result_table.show())
    print("read_df", read_df.show())

    assert_dataframes_are_equal(read_df, result_table)

    spark.sql("DROP TABLE IF EXISTS meta.dummy_table_for_unit_test")
    spark.sql("DROP TABLE IF EXISTS meta.dummy_watermark_table_for_unit_test")
