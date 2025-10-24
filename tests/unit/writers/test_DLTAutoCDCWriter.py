import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from writers.DLTAutoCDCWriter import DLTAutoCDCWriter
from tests.unit.writers.test_base_dlt_writer import BaseDLTWriterTest


class TestDLTAutoCDCWriter(BaseDLTWriterTest):
    """Test cases for DLTAutoCDCWriter class"""

    @pytest.fixture
    def entity_config(self):
        """Entity configuration for DLT auto CDC"""
        return {
            "source": {
                "reader_type": "table_stream",
                "table_name": "source_schema.customers_raw"
            },
            "target": {
                "writer_type": "dlt_auto_cdc",
                "table_name": "target_schema.customers",
                "keys": ["customer_id"],
                "sequence_by": "updated_timestamp",
                "ignore_null_updates": False,
                "apply_as_deletes": "operation = 'DELETE'",
                "apply_as_truncates": "operation = 'TRUNCATE'",
                "column_list": ["customer_id", "name", "email", "updated_timestamp"],
                "stored_as_scd_type": "2",
                "track_history_column_list": ["name", "email"],
                "flow_name": "customer_cdc_flow",
                "once": False
            }
        }

    @pytest.fixture
    def writer(self, entity_config, mock_spark, mock_dbutils, mock_logger):
        """Create DLTAutoCDCWriter instance"""
        config = EntityConfig(**entity_config)
        return DLTAutoCDCWriter(config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, writer, mock_spark, mock_dbutils):
        """Test initialization of DLTAutoCDCWriter"""
        assert writer.spark == mock_spark
        assert writer.dbutils == mock_dbutils
        assert writer.entity_config.target.writer_type == "dlt_auto_cdc"
        assert writer.entity_config.target.table_name == "target_schema.customers"

    @patch('writers.DLTAutoCDCWriter.dlt')
    def test_execute_declarative_operation(self, mock_dlt, writer, mock_dataframe):
        """Test _execute_declarative_operation method"""
        mock_dlt.create_auto_cdc_flow.return_value = "mock_cdc_flow_result"
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.create_auto_cdc_flow.assert_called_once_with(
            target="target_schema.customers",
            source="source_schema.customers_raw",
            keys=["customer_id"],
            sequence_by="updated_timestamp",
            ignore_null_updates=False,
            apply_as_deletes="operation = 'DELETE'",
            apply_as_truncates="operation = 'TRUNCATE'",
            column_list=["customer_id", "name", "email", "updated_timestamp"],
            except_column_list=None,
            stored_as_scd_type="2",
            track_history_column_list=["name", "email"],
            track_history_except_column_list=None,
            name="customer_cdc_flow",
            once=False
        )
        assert result == "mock_cdc_flow_result"

    @patch('writers.DLTAutoCDCWriter.dlt')
    def test_execute_declarative_operation_minimal_config(self, mock_dlt, mock_spark, mock_dbutils, mock_logger, mock_dataframe):
        """Test _execute_declarative_operation with minimal configuration"""
        minimal_config = {
            "source": {
                "reader_type": "table_stream",
                "table_name": "test_schema.test_source"
            },
            "target": {
                "writer_type": "dlt_auto_cdc",
                "table_name": "test_schema.test_table",
                "keys": ["id"]
            }
        }
        config = EntityConfig(**minimal_config)
        writer = DLTAutoCDCWriter(config, mock_spark, mock_dbutils, mock_logger)
        
        mock_dlt.create_auto_cdc_flow.return_value = "mock_result"
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.create_auto_cdc_flow.assert_called_once_with(
            target="test_schema.test_table",
            source="test_schema.test_source",
            keys=["id"],
            sequence_by=None,
            ignore_null_updates=False,
            apply_as_deletes=None,
            apply_as_truncates=None,
            column_list=None,
            except_column_list=None,
            stored_as_scd_type=None,
            track_history_column_list=None,
            track_history_except_column_list=None,
            name=None,
            once=False
        )
        assert result == "mock_result"

    def test_inheritance(self, writer):
        """Test that DLTAutoCDCWriter properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer):
        """Test that DLTAutoCDCWriter implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer, IDataWriter)
