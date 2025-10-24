import pytest
from unittest.mock import patch, MagicMock
from readers.FileStreamReader import FileStreamReader
from core_functions.config_models import EntityConfig

# Test variables
catalog_name = "test_catalog"
schema_volume_path = "test_path"
volume_path = f"/Volumes/{catalog_name}/{schema_volume_path}"
schema_checkpoint_return_value = ["test_schema", "test_checkpoint"]


def test_add_file_metadata(databricks_spark, dbutils_mock):
    entity_config = {"entity_name": "test_entity"}
    entity_config = EntityConfig(**entity_config)

    file_reader = FileStreamReader(entity_config, databricks_spark, dbutils_mock)

    mock_data = [
        (
            "value1",
            {
                "file_name": "test_file_name",
                "file_modification_time": "test_file_modification_time",
            },
        )
    ]
    mock_columns = ["value", "_metadata"]
    mock_df = databricks_spark.createDataFrame(mock_data, mock_columns)

    result_df = file_reader._add_file_metadata(mock_df)

    assert "md_validation_status" in result_df.columns
    assert "md_test_entity_pk" in result_df.columns
    assert "md_metadata" in result_df.columns
    assert "md_source_name" in result_df.columns
    assert "md_file_modification_time" in result_df.columns
    assert "md_created_datetime" in result_df.columns
    assert "md_updated_datetime" in result_df.columns
    assert "md_deleted_datetime" in result_df.columns
    assert "md_change_status" in result_df.columns
    assert result_df.collect()[0]["md_source_name"] == "test_file_name"
    assert (
        result_df.collect()[0]["md_file_modification_time"]
        == "test_file_modification_time"
    )


@patch("core_functions.utils.get_source_volume_path", return_value=volume_path)
@patch(
    "core_functions.utils.get_schema_location",
    return_value=schema_checkpoint_return_value,
)
@patch("readers.FileStreamReader.FileStreamReader._add_file_metadata")
def test_read_source_to_dataframe(
    mock__add_file_metadata,
    mock_get_schema_location,
    mock_get_source_volume_path,
    spark_mock,
    dbutils_mock,
):
    entity_config = {
        "source": {
            "reader_type": "file_stream",
            "format": "parquet",
            "read_options": {"header": "true"},
            "volume_path": "test_volume_path",
        }
    }
    entity_config = EntityConfig(**entity_config)
    file_reader = FileStreamReader(entity_config, spark_mock, dbutils_mock)
    file_reader.read_source_to_dataframe(entity_config, spark_mock)
    spark_mock.readStream.format.assert_called_once_with("cloudFiles")
    spark_mock.readStream.format.return_value.option.assert_called_once_with(
        "cloudFiles.format", "parquet"
    )
