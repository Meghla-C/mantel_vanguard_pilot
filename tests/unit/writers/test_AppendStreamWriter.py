import pytest
from pyspark.sql import DataFrame
from unittest.mock import patch, MagicMock, Mock
from writers.AppendStreamWriter import AppendStreamWriter
from core_functions.utils import get_checkpoint_location
from core_functions.config_models import EntityConfig


@pytest.fixture
def mock_logger():
    """Mock logger fixture"""
    return Mock()


@patch(
    "core_functions.utils.get_source_volume_path",
    return_value="/Volumes/test_catalog/test_path",
)
@patch("core_functions.utils.get_checkpoint_location", return_value="test_checkpoint")
def test_invalid_table_name(
    mock_get_checkpoint_location, mock_get_source_volume_path, spark_mock, dbutils_mock, mock_logger
):
    entity_config = {
        "entity_name": "test_entity",
        "target": {
            "writer_type": "append_stream",
            "table_name": "test_schema",
            "trigger_type_value": {"availableNow": True},
        },
    }
    with pytest.raises(
        ValueError,
        match="target_table_name with its schema name is required",
    ):
        asw = AppendStreamWriter(
            EntityConfig(**entity_config), spark_mock, dbutils_mock, mock_logger
        )
        asw.write_to_target_table(None)


@patch(
    "core_functions.utils.get_source_volume_path",
    return_value="/Volumes/test_catalog/test_path",
)
@patch("core_functions.utils.get_checkpoint_location", return_value="test_checkpoint")
def test_append_stream_writer(
    mock_get_checkpoint_location, mock_get_source_volume_path, spark_mock, dbutils_mock, mock_logger
):
    mock_source_df = MagicMock()
    
    # Set up the mock chain properly
    mock_write_stream = MagicMock()
    mock_format = MagicMock()
    mock_option = MagicMock()
    mock_output_mode = MagicMock()
    mock_trigger = MagicMock()
    mock_streaming_query = MagicMock()
    
    mock_source_df.writeStream = mock_write_stream
    mock_write_stream.format.return_value = mock_format
    mock_format.option.return_value = mock_option
    mock_option.outputMode.return_value = mock_output_mode
    mock_output_mode.trigger.return_value = mock_trigger
    mock_trigger.toTable.return_value = mock_streaming_query

    entity_config = {
        "entity_name": "test_entity",
        "target": {
            "writer_type": "append_stream",
            "table_name": "test_schema.test_table",
            "trigger_type_value": {"availableNow": True},
        },
    }

    asw = AppendStreamWriter(EntityConfig(**entity_config), spark_mock, dbutils_mock, mock_logger)
    asw.write_to_target_table(mock_source_df)

    # Verify logging calls
    mock_logger.info.assert_any_call("Starting streaming write operation to target table: test_schema.test_table")
    mock_logger.info.assert_any_call("Using checkpoint location: test_checkpoint")
    mock_logger.info.assert_any_call("Configuring streaming write with format=delta, outputMode=append")
    mock_logger.info.assert_any_call("Streaming query started for table: test_schema.test_table")
    mock_logger.info.assert_any_call("Waiting for streaming query termination...")
    mock_logger.info.assert_any_call("Streaming write operation completed successfully for table: test_schema.test_table")

    # Verify streaming configuration calls
    mock_write_stream.format.assert_called_once_with("delta")
    mock_format.option.assert_called_once_with("checkpointLocation", "test_checkpoint")
    mock_option.outputMode.assert_called_once_with("append")
    mock_output_mode.trigger.assert_called_once_with(**{"availableNow": True})
    mock_trigger.toTable.assert_called_once_with("test_schema.test_table")
    
    # Verify awaitTermination was called
    mock_streaming_query.awaitTermination.assert_called_once()
