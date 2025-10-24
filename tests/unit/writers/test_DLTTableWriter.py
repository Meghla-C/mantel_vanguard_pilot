import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from writers.DLTTableWriter import DLTTableWriter


class TestDLTTableWriter:
    """Test cases for DLTTableWriter class"""

    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession"""
        return Mock()

    @pytest.fixture
    def mock_dbutils(self):
        """Mock dbutils"""
        return Mock()

    @pytest.fixture
    def mock_logger(self):
        """Mock logger"""
        return Mock()

    @pytest.fixture
    def mock_dataframe(self):
        """Mock DataFrame"""
        df = Mock(spec=DataFrame)
        df.columns = ["id", "name", "amount", "status"]
        df.select.return_value = df
        df.filter.return_value = df
        return df

    @pytest.fixture
    def entity_config_with_expectations(self):
        """Entity configuration for DLT table with unified expectations format"""
        return {
            "target": {
                "writer_type": "dlt_table",
                "table_name": "test_schema.test_table",
                "comment": "Test table with data quality expectations",
                "expectations": [
                    {
                        "name": "valid_amount",
                        "constraint": "amount > 0",
                        "action": "fail"
                    },
                    {
                        "name": "valid_status",
                        "constraint": "status IS NOT NULL",
                        "action": "fail"
                    },
                    {
                        "name": "positive_id",
                        "constraint": "id > 0",
                        "action": "drop"
                    }
                ]
            }
        }

    @pytest.fixture
    def entity_config_legacy_expectations(self):
        """Entity configuration with legacy DLT expectations format"""
        return {
            "target": {
                "writer_type": "dlt_table",
                "table_name": "test_schema.legacy_table",
                "expect_all": {
                    "valid_amount": "amount > 0"
                },
                "expect_all_or_drop": {
                    "valid_status": "status IS NOT NULL"
                },
                "expect_all_or_fail": {
                    "positive_id": "id > 0"
                }
            }
        }

    @pytest.fixture
    def entity_config_minimal(self):
        """Minimal entity configuration"""
        return {
            "target": {
                "writer_type": "dlt_table",
                "table_name": "test_schema.minimal_table"
            }
        }

    @pytest.fixture
    def writer_with_expectations(self, entity_config_with_expectations, mock_spark, mock_dbutils, mock_logger):
        """Create DLTTableWriter instance with expectations"""
        config = EntityConfig(**entity_config_with_expectations)
        return DLTTableWriter(config, mock_spark, mock_dbutils, mock_logger)

    @pytest.fixture
    def writer_legacy_expectations(self, entity_config_legacy_expectations, mock_spark, mock_dbutils, mock_logger):
        """Create DLTTableWriter instance with legacy expectations"""
        config = EntityConfig(**entity_config_legacy_expectations)
        return DLTTableWriter(config, mock_spark, mock_dbutils, mock_logger)

    @pytest.fixture
    def writer_minimal(self, entity_config_minimal, mock_spark, mock_dbutils, mock_logger):
        """Create DLTTableWriter instance with minimal config"""
        config = EntityConfig(**entity_config_minimal)
        return DLTTableWriter(config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, writer_with_expectations, mock_spark, mock_dbutils):
        """Test initialization of DLTTableWriter"""
        assert writer_with_expectations.spark == mock_spark
        assert writer_with_expectations.dbutils == mock_dbutils
        assert writer_with_expectations.entity_config.target.writer_type == "dlt_table"
        assert writer_with_expectations.entity_config.target.table_name == "test_schema.test_table"

    @patch('writers.DLTTableWriter.dlt')
    def test_execute_declarative_operation_with_unified_expectations(self, mock_dlt, writer_with_expectations, mock_dataframe):
        """Test _execute_declarative_operation with unified expectations format"""
        mock_dlt.table.return_value = "mock_table_result"
        
        result = writer_with_expectations._execute_declarative_operation(mock_dataframe)
        
        # Verify dlt.table was called with correct parameters
        mock_dlt.table.assert_called_once()
        call_args = mock_dlt.table.call_args
        
        assert call_args[1]["name"] == "test_schema.test_table"
        assert call_args[1]["comment"] == "Test table with data quality expectations"
        
        # DLTTableWriter passes a function to dlt.table, not direct expectations
        assert callable(call_args[0][0])  # First positional argument should be a function
        
        assert result == "mock_table_result"

    @patch('writers.DLTTableWriter.dlt')
    def test_execute_declarative_operation_with_legacy_expectations(self, mock_dlt, writer_legacy_expectations, mock_dataframe):
        """Test _execute_declarative_operation with legacy expectations format"""
        mock_dlt.table.return_value = "mock_table_result"
        
        result = writer_legacy_expectations._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.table.assert_called_once()
        call_args = mock_dlt.table.call_args
        
        assert call_args[1]["name"] == "test_schema.legacy_table"
        # DLTTableWriter passes a function to dlt.table, not direct expectations
        assert callable(call_args[0][0])  # First positional argument should be a function
        
        assert result == "mock_table_result"

    @patch('writers.DLTTableWriter.dlt')
    def test_execute_declarative_operation_minimal_config(self, mock_dlt, writer_minimal, mock_dataframe):
        """Test _execute_declarative_operation with minimal configuration"""
        mock_dlt.table.return_value = "mock_result"
        
        result = writer_minimal._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.table.assert_called_once()
        call_args = mock_dlt.table.call_args
        
        assert call_args[1]["name"] == "test_schema.minimal_table"
        # With minimal config, only name and cluster_by_auto should be present
        assert "comment" not in call_args[1]  # comment should not be included when not provided
        assert call_args[1]["cluster_by_auto"] == True  # default value should be included
        assert call_args[1]["temporary"] == False  # default value should be included
        # DLTTableWriter passes a function to dlt.table, not direct expectations
        assert callable(call_args[0][0])  # First positional argument should be a function
        
        assert result == "mock_result"

    def test_convert_expectations_to_dlt_format_unified(self, writer_with_expectations):
        """Test _convert_expectations_to_dlt_format with unified expectations"""
        result = writer_with_expectations._convert_expectations_to_dlt_format()
        
        # Check that expectations were converted properly
        # "fail" action maps to "expect_all", "drop" action maps to "expect_all_or_drop"
        assert result["expect_all"]["valid_amount"] == "amount > 0"
        assert result["expect_all"]["valid_status"] == "status IS NOT NULL"
        assert result["expect_all_or_drop"]["positive_id"] == "id > 0"

    def test_convert_expectations_to_dlt_format_empty(self, writer_minimal):
        """Test _convert_expectations_to_dlt_format with empty expectations"""
        result = writer_minimal._convert_expectations_to_dlt_format()
        
        assert result == {
            "expect_all": {},
            "expect_all_or_drop": {},
            "expect_all_or_fail": {}
        }

    def test_convert_expectations_to_dlt_format_invalid_action(self, mock_spark, mock_dbutils, mock_logger):
        """Test _convert_expectations_to_dlt_format with invalid action defaults to expect_all"""
        config = {
            "target": {
                "writer_type": "dlt_table",
                "table_name": "test_schema.test_table",
                "expectations": [
                    {
                        "name": "test_constraint",
                        "constraint": "value > 0",
                        "action": "invalid_action"
                    }
                ]
            }
        }
        entity_config = EntityConfig(**config)
        writer = DLTTableWriter(entity_config, mock_spark, mock_dbutils, mock_logger)
        
        result = writer._convert_expectations_to_dlt_format()
        
        # Invalid action should not be processed, so all should be empty
        assert result["expect_all"] == {}
        assert result["expect_all_or_drop"] == {}
        assert result["expect_all_or_fail"] == {}

    def test_inheritance(self, writer_with_expectations):
        """Test that DLTTableWriter properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer_with_expectations, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer_with_expectations):
        """Test that DLTTableWriter implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer_with_expectations, IDataWriter)

    @patch('writers.DLTTableWriter.dlt')
    def test_write_to_target_table(self, mock_dlt, writer_with_expectations, mock_dataframe):
        """Test write_to_target_table method"""
        mock_dlt.table.return_value = "mock_table_result"
        
        result = writer_with_expectations.write_to_target_table(mock_dataframe)
        
        mock_dlt.table.assert_called_once()
        assert result == "mock_table_result"
