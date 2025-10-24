import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from writers.DLTAutoCDCSnapshotWriter import DLTAutoCDCSnapshotWriter
from tests.unit.writers.test_base_dlt_writer import BaseDLTWriterTest


class TestDLTAutoCDCSnapshotWriter(BaseDLTWriterTest):
    """Test cases for DLTAutoCDCSnapshotWriter class"""

    @pytest.fixture
    def entity_config(self):
        """Entity configuration for DLT auto CDC snapshot"""
        return {
            "source": {
                "reader_type": "table_stream",
                "table_name": "source_schema.products_raw"
            },
            "target": {
                "writer_type": "dlt_auto_cdc_snapshot",
                "table_name": "target_schema.products_history",
                "keys": ["product_id"],
                "stored_as_scd_type": "2",
                "track_history_column_list": ["product_name", "price", "category"],
                "track_history_except_column_list": ["internal_notes"]
            }
        }

    @pytest.fixture
    def writer(self, entity_config, mock_spark, mock_dbutils, mock_logger):
        """Create DLTAutoCDCSnapshotWriter instance"""
        config = EntityConfig(**entity_config)
        return DLTAutoCDCSnapshotWriter(config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, writer, mock_spark, mock_dbutils):
        """Test initialization of DLTAutoCDCSnapshotWriter"""
        assert writer.spark == mock_spark
        assert writer.dbutils == mock_dbutils
        assert writer.entity_config.target.writer_type == "dlt_auto_cdc_snapshot"
        assert writer.entity_config.target.table_name == "target_schema.products_history"

    @patch('writers.DLTAutoCDCSnapshotWriter.dlt')
    def test_execute_declarative_operation(self, mock_dlt, writer, mock_dataframe):
        """Test _execute_declarative_operation method"""
        mock_dlt.create_auto_cdc_from_snapshot_flow.return_value = "mock_snapshot_cdc_result"
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.create_auto_cdc_from_snapshot_flow.assert_called_once_with(
            target="target_schema.products_history",
            source="source_schema.products_raw",
            keys=["product_id"],
            stored_as_scd_type="2",
            track_history_column_list=["product_name", "price", "category"],
            track_history_except_column_list=["internal_notes"]
        )
        assert result == "mock_snapshot_cdc_result"

    @patch('writers.DLTAutoCDCSnapshotWriter.dlt')
    def test_execute_declarative_operation_minimal_config(self, mock_dlt, mock_spark, mock_dbutils, mock_logger, mock_dataframe):
        """Test _execute_declarative_operation with minimal configuration"""
        minimal_config = {
            "source": {
                "reader_type": "table_stream",
                "table_name": "test_schema.test_source"
            },
            "target": {
                "writer_type": "dlt_auto_cdc_snapshot",
                "table_name": "test_schema.test_table",
                "keys": ["id"]
            }
        }
        config = EntityConfig(**minimal_config)
        writer = DLTAutoCDCSnapshotWriter(config, mock_spark, mock_dbutils, mock_logger)
        
        mock_dlt.create_auto_cdc_from_snapshot_flow.return_value = "mock_result"
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.create_auto_cdc_from_snapshot_flow.assert_called_once_with(
            target="test_schema.test_table",
            source="test_schema.test_source",
            keys=["id"],
            stored_as_scd_type=None,
            track_history_column_list=None,
            track_history_except_column_list=None
        )
        assert result == "mock_result"

    def test_inheritance(self, writer):
        """Test that DLTAutoCDCSnapshotWriter properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer):
        """Test that DLTAutoCDCSnapshotWriter implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer, IDataWriter)
