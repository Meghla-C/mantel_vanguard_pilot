import pytest
from readers.TableStreamReader import TableStreamReader
from core_functions.config_models import EntityConfig


def test_add_table_metadata_with_previous_metadata(databricks_spark, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "table_stream",
            "table_name": "ingest_placeholder.test_table",
        },
    }

    tr = TableStreamReader(
        EntityConfig(**entity_config), databricks_spark, dbutils_mock
    )

    mock_data = [
        (
            "value1",
            "None",
            "test_entity_pk1",
            "metadata1",
            "file_name1",
            "file_modification_time1",
            "created_datetime1",
            "updated_datetime1",
            "deleted_datetime1",
            "change_status1",
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
    mock_df = databricks_spark.createDataFrame(mock_data, mock_columns)

    result_df = tr._add_table_metadata(mock_df)

    assert "md_validation_status_origin" in result_df.columns
    assert "md_test_entity_pk_origin" in result_df.columns
    assert "md_metadata_origin" in result_df.columns
    assert "md_source_name_origin" in result_df.columns
    assert "md_created_datetime_origin" in result_df.columns
    assert "md_updated_datetime_origin" in result_df.columns
    assert "md_deleted_datetime_origin" in result_df.columns
    assert "md_change_status_origin" in result_df.columns
    assert "md_validation_status" in result_df.columns
    assert "md_test_entity_pk" in result_df.columns
    assert "md_metadata" in result_df.columns
    assert "md_source_name" in result_df.columns
    assert "md_created_datetime" in result_df.columns
    assert "md_updated_datetime" in result_df.columns
    assert "md_deleted_datetime" in result_df.columns
    assert "md_change_status" in result_df.columns


def test_invalid_table_name(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "table_stream",
            "table_name": "test_schema",
            "read_options": {},
            "filter_condition": "test_column = '1'",
        },
    }
    with pytest.raises(ValueError, match="Full table name with schema is required"):
        tr = TableStreamReader(EntityConfig(**entity_config), spark_mock, dbutils_mock)
        tr.read_source_to_dataframe(None)


def test_table_stream_reader_with_filter(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "table_stream",
            "table_name": "ingest_placeholder.test_table",
            "read_options": {},
            "filter_condition": "test_column = '1'",
        },
    }

    tr = TableStreamReader(EntityConfig(**entity_config), spark_mock, dbutils_mock)
    tr.read_source_to_dataframe()

    spark_mock.readStream.options.assert_called_once_with()
    spark_mock.readStream.options().table.assert_called_once_with(
        "ingest_placeholder.test_table"
    )
    spark_mock.readStream.options().table().filter.assert_called_once_with(
        "test_column = '1'"
    )


def test_table_stream_reader_without_filter(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "table_stream",
            "table_name": "ingest_placeholder.test_table",
            "read_options": {},
            "filter_condition": "",
        },
    }

    tr = TableStreamReader(EntityConfig(**entity_config), spark_mock, dbutils_mock)
    tr.read_source_to_dataframe()

    spark_mock.readStream.options.assert_called_once_with()
    spark_mock.readStream.options().table.assert_called_once_with(
        "ingest_placeholder.test_table"
    )
    spark_mock.readStream.options().table().filter.assert_not_called()
