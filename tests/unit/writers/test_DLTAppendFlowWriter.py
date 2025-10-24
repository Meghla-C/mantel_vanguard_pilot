import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from writers.DLTAppendFlowWriter import DLTAppendFlowWriter
from tests.unit.writers.test_base_dlt_writer import BaseDLTWriterTest


class TestDLTAppendFlowWriter(BaseDLTWriterTest):
    """Test cases for DLTAppendFlowWriter class"""

    @pytest.fixture
    def entity_config(self):
        """Entity configuration for DLT append flow"""
        return {
            "target": {
                "writer_type": "dlt_append_flow",
                "table_name": "sales_schema.sales_transactions",
                "flow_name": "sales_append_flow",
                "once": False,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                },
                "comment": "Sales transactions table updated via DLT append flow"
            }
        }

    @pytest.fixture
    def writer(self, entity_config, mock_spark, mock_dbutils, mock_logger):
        """Create DLTAppendFlowWriter instance"""
        config = EntityConfig(**entity_config)
        return DLTAppendFlowWriter(config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, writer, mock_spark, mock_dbutils):
        """Test initialization of DLTAppendFlowWriter"""
        assert writer.spark == mock_spark
        assert writer.dbutils == mock_dbutils
        assert writer.entity_config.target.writer_type == "dlt_append_flow"
        assert writer.entity_config.target.table_name == "sales_schema.sales_transactions"

    @patch('writers.DLTAppendFlowWriter.dlt')
    def test_execute_declarative_operation(self, mock_dlt, writer, mock_dataframe):
        """Test _execute_declarative_operation method"""
        # Mock append_flow to return a decorator function that returns a result when called
        mock_decorator = Mock()
        mock_decorator.return_value = "mock_append_flow_result"
        mock_dlt.append_flow.return_value = mock_decorator
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.append_flow.assert_called_once_with(
            target="sales_schema.sales_transactions",
            name="sales_append_flow",
            once=False,
            spark_conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        )
        # Verify the decorator was called with _prepare_data
        mock_decorator.assert_called_once_with(writer._prepare_data)
        assert result == "mock_append_flow_result"

    @patch('writers.DLTAppendFlowWriter.dlt')
    def test_execute_declarative_operation_minimal_config(self, mock_dlt, mock_spark, mock_dbutils, mock_logger, mock_dataframe):
        """Test _execute_declarative_operation with minimal configuration"""
        minimal_config = {
            "target": {
                "writer_type": "dlt_append_flow",
                "table_name": "test_schema.test_table"
            }
        }
        config = EntityConfig(**minimal_config)
        writer = DLTAppendFlowWriter(config, mock_spark, mock_dbutils, mock_logger)
        
        # Mock append_flow to return a decorator function that returns a result when called
        mock_decorator = Mock()
        mock_decorator.return_value = "mock_result"
        mock_dlt.append_flow.return_value = mock_decorator
        
        result = writer._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.append_flow.assert_called_once_with(
            target="test_schema.test_table",
            name="test_schema_test_table",
            once=False,
            spark_conf={}
        )
        # Verify the decorator was called with _prepare_data
        mock_decorator.assert_called_once_with(writer._prepare_data)
        assert result == "mock_result"

    @patch('writers.DLTAppendFlowWriter.dlt')
    def test_write_to_target_table_full_workflow(self, mock_dlt, writer, mock_dataframe):
        """Test complete write_to_target_table workflow"""
        # Mock the _create_table_if_not_exists method to avoid DLT calls in base class
        with patch.object(writer, '_create_table_if_not_exists') as mock_create_table:
            # Mock append_flow decorator
            mock_flow_decorator = Mock()
            mock_flow_decorator.return_value = "mock_append_flow_result"
            mock_dlt.append_flow.return_value = mock_flow_decorator
            
            result = writer.write_to_target_table(mock_dataframe)
            
            # Verify table creation was called
            mock_create_table.assert_called_once()
            # Verify DLT append_flow was called
            mock_dlt.append_flow.assert_called_once()
            mock_flow_decorator.assert_called_once_with(writer._prepare_data)
            assert result == "mock_append_flow_result"

    @patch('writers.DLTAppendFlowWriter.dlt')
    def test_write_to_target_table_with_custom_params(self, mock_dlt, writer, mock_dataframe):
        """Test write_to_target_table with custom spark and dbutils parameters"""
        custom_spark = Mock()
        custom_dbutils = Mock()
        
        # Mock create_streaming_table decorator
        mock_table_decorator = Mock()
        mock_table_decorator.return_value = "mock_table"
        mock_dlt.create_streaming_table.return_value = mock_table_decorator
        
        # Mock append_flow decorator
        mock_flow_decorator = Mock()
        mock_flow_decorator.return_value = "mock_result"
        mock_dlt.append_flow.return_value = mock_flow_decorator
        
        result = writer.write_to_target_table(
            mock_dataframe, 
            spark=custom_spark, 
            dbutils=custom_dbutils
        )
        
        # Should still use the original spark and dbutils from init
        assert result == "mock_result"
        mock_dlt.append_flow.assert_called_once()
        mock_flow_decorator.assert_called_once_with(writer._prepare_data)

    def test_inheritance(self, writer):
        """Test that DLTAppendFlowWriter properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer):
        """Test that DLTAppendFlowWriter implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer, IDataWriter)
        
        # Check required methods exist
        assert hasattr(writer, '__init__')
        assert hasattr(writer, 'write_to_target_table')
        assert callable(getattr(writer, 'write_to_target_table'))
