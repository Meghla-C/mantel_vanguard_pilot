import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from writers.DLTSinkWriter import DLTSinkWriter
from tests.unit.writers.test_base_dlt_writer import BaseDLTWriterTest


class TestDLTSinkWriter(BaseDLTWriterTest):
    """Test cases for DLTSinkWriter class"""

    @pytest.fixture
    def entity_config(self):
        """Entity configuration for DLT sink"""
        return {
            "target": {
                "writer_type": "dlt_sink",
                "table_name": "export_schema.data_export",
                "sink_name": "data_export_sink",
                "format": "parquet",
                "options": {
                    "path": "/mnt/exports/processed_data",
                    "mode": "overwrite",
                    "compression": "snappy",
                    "partitionBy": "export_date"
                }
            }
        }

    @pytest.fixture
    def writer(self, entity_config, mock_spark, mock_dbutils, mock_logger):
        """Create DLTSinkWriter instance"""
        config = EntityConfig(**entity_config)
        return DLTSinkWriter(config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, writer, mock_spark, mock_dbutils):
        """Test initialization of DLTSinkWriter"""
        assert writer.spark == mock_spark
        assert writer.dbutils == mock_dbutils
        assert writer.entity_config.target.writer_type == "dlt_sink"
        assert writer.entity_config.target.table_name == "export_schema.data_export"

    @patch('writers.DLTSinkWriter.dlt')
    def test_execute_declarative_operation(self, mock_dlt, writer, mock_dataframe):
        """Test _execute_declarative_operation method"""
        mock_dlt.create_sink.return_value = "mock_sink_result"
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.create_sink.assert_called_once_with(
            name="data_export_sink",
            format="parquet",
            options={
                "path": "/mnt/exports/processed_data",
                "mode": "overwrite",
                "compression": "snappy",
                "partitionBy": "export_date"
            }
        )
        assert result == "mock_sink_result"

    @patch('writers.DLTSinkWriter.dlt')
    def test_execute_declarative_operation_minimal_config(self, mock_dlt, mock_spark, mock_dbutils, mock_logger, mock_dataframe):
        """Test _execute_declarative_operation with minimal configuration"""
        minimal_config = {
            "target": {
                "writer_type": "dlt_sink",
                "table_name": "test_schema.test_table",
                "sink_name": "test_sink",
                "format": "delta"
            }
        }
        config = EntityConfig(**minimal_config)
        writer = DLTSinkWriter(config, mock_spark, mock_dbutils, mock_logger)
        
        mock_dlt.create_sink.return_value = "mock_result"
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.create_sink.assert_called_once_with(
            name="test_sink",
            format="delta",
            options={}
        )
        assert result == "mock_result"

    def test_inheritance(self, writer):
        """Test that DLTSinkWriter properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer):
        """Test that DLTSinkWriter implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer, IDataWriter)
