import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from writers.DLTViewWriter import DLTViewWriter


class TestDLTViewWriter:
    """Test cases for DLTViewWriter class"""

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
        """Entity configuration for DLT view with unified expectations format"""
        return {
            "target": {
                "writer_type": "dlt_view",
                "view_name": "test_schema.test_view",
                "comment": "Test view with data quality expectations",
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
    def entity_config_with_table_properties(self):
        """Entity configuration with table properties"""
        return {
            "target": {
                "writer_type": "dlt_view",
                "view_name": "test_schema.test_view_props",
                "comment": "Test view with table properties",
                "table_properties": {
                    "quality": "gold",
                    "owner": "data_team"
                }
            }
        }

    @pytest.fixture
    def entity_config_minimal(self):
        """Minimal entity configuration"""
        return {
            "target": {
                "writer_type": "dlt_view",
                "view_name": "test_schema.minimal_view"
            }
        }

    @pytest.fixture
    def writer_with_expectations(self, entity_config_with_expectations, mock_spark, mock_dbutils, mock_logger):
        """Create DLTViewWriter instance with expectations"""
        config = EntityConfig(**entity_config_with_expectations)
        return DLTViewWriter(config, mock_spark, mock_dbutils, mock_logger)

    @pytest.fixture
    def writer_with_table_properties(self, entity_config_with_table_properties, mock_spark, mock_dbutils, mock_logger):
        """Create DLTViewWriter instance with table properties"""
        config = EntityConfig(**entity_config_with_table_properties)
        return DLTViewWriter(config, mock_spark, mock_dbutils, mock_logger)

    @pytest.fixture
    def writer_minimal(self, entity_config_minimal, mock_spark, mock_dbutils, mock_logger):
        """Create DLTViewWriter instance with minimal config"""
        config = EntityConfig(**entity_config_minimal)
        return DLTViewWriter(config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, writer_with_expectations, mock_spark, mock_dbutils):
        """Test initialization of DLTViewWriter"""
        assert writer_with_expectations.spark == mock_spark
        assert writer_with_expectations.dbutils == mock_dbutils
        assert writer_with_expectations.entity_config.target.writer_type == "dlt_view"
        assert writer_with_expectations.entity_config.target.view_name == "test_schema.test_view"

    @patch('writers.DLTViewWriter.dlt')
    def test_execute_declarative_operation_with_expectations(self, mock_dlt, writer_with_expectations, mock_dataframe):
        """Test _execute_declarative_operation with expectations"""
        mock_dlt.view.return_value = "mock_view_result"
        
        result = writer_with_expectations._execute_declarative_operation(mock_dataframe)
        
        # Verify dlt.view was called with correct parameters
        mock_dlt.view.assert_called_once()
        call_args = mock_dlt.view.call_args
        
        assert call_args[1]["name"] == "test_schema.test_view"
        assert call_args[1]["comment"] == "Test view with data quality expectations"
        
        # DLTViewWriter passes a function to dlt.view, not direct expectations
        assert callable(call_args[0][0])  # First positional argument should be a function
        
        assert result == "mock_view_result"

    @patch('writers.DLTViewWriter.dlt')
    def test_execute_declarative_operation_with_table_properties(self, mock_dlt, writer_with_table_properties, mock_dataframe):
        """Test _execute_declarative_operation with table properties"""
        mock_dlt.view.return_value = "mock_view_result"
        
        result = writer_with_table_properties._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.view.assert_called_once()
        call_args = mock_dlt.view.call_args
        
        assert call_args[1]["name"] == "test_schema.test_view_props"
        assert call_args[1]["comment"] == "Test view with table properties"
        assert call_args[1]["table_properties"] == {"quality": "gold", "owner": "data_team"}
        
        # DLTViewWriter passes a function to dlt.view
        assert callable(call_args[0][0])  # First positional argument should be a function
        
        assert result == "mock_view_result"

    @patch('writers.DLTViewWriter.dlt')
    def test_execute_declarative_operation_minimal_config(self, mock_dlt, writer_minimal, mock_dataframe):
        """Test _execute_declarative_operation with minimal configuration"""
        mock_dlt.view.return_value = "mock_result"
        
        result = writer_minimal._execute_declarative_operation(mock_dataframe)
        
        mock_dlt.view.assert_called_once()
        call_args = mock_dlt.view.call_args
        
        assert call_args[1]["name"] == "test_schema.minimal_view"
        # Should not include comment or table_properties if not set
        assert "comment" not in call_args[1] or call_args[1]["comment"] is None
        assert "table_properties" not in call_args[1] or call_args[1]["table_properties"] is None
        
        # DLTViewWriter passes a function to dlt.view
        assert callable(call_args[0][0])  # First positional argument should be a function
        
        assert result == "mock_result"

    @patch('writers.DLTViewWriter.DLT_AVAILABLE', False)
    def test_execute_declarative_operation_dlt_not_available(self, writer_minimal, mock_dataframe, mock_logger):
        """Test _execute_declarative_operation when DLT is not available"""
        result = writer_minimal._execute_declarative_operation(mock_dataframe)
        
        assert result is None
        mock_logger.warning.assert_called_with(
            "DLT is not available in this environment. DLTViewWriter cannot be used outside of a DLT pipeline context."
        )

    def test_inheritance(self, writer_with_expectations):
        """Test that DLTViewWriter properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer_with_expectations, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer_with_expectations):
        """Test that DLTViewWriter implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer_with_expectations, IDataWriter)

    @patch('writers.DLTViewWriter.dlt')
    def test_write_to_target_table(self, mock_dlt, writer_with_expectations, mock_dataframe):
        """Test write_to_target_table method"""
        mock_dlt.view.return_value = "mock_view_result"
        
        result = writer_with_expectations.write_to_target_table(mock_dataframe)
        
        mock_dlt.view.assert_called_once()
        assert result == "mock_view_result"

    def test_view_config_extraction_empty_comment(self, mock_spark, mock_dbutils, mock_logger):
        """Test view configuration extraction when comment is empty"""
        config = {
            "target": {
                "writer_type": "dlt_view",
                "view_name": "test_schema.test_view",
                "comment": ""  # Empty comment
            }
        }
        entity_config = EntityConfig(**config)
        writer = DLTViewWriter(entity_config, mock_spark, mock_dbutils, mock_logger)
        
        with patch('writers.DLTViewWriter.dlt') as mock_dlt:
            mock_dlt.view.return_value = "mock_result"
            mock_dataframe = Mock(spec=DataFrame)
            
            writer._execute_declarative_operation(mock_dataframe)
            
            call_args = mock_dlt.view.call_args
            # Empty comment should not be included in view_config
            assert "comment" not in call_args[1] or not call_args[1]["comment"]

    def test_view_config_extraction_empty_table_properties(self, mock_spark, mock_dbutils, mock_logger):
        """Test view configuration extraction when table_properties is empty"""
        config = {
            "target": {
                "writer_type": "dlt_view",
                "view_name": "test_schema.test_view",
                "table_properties": {}  # Empty table properties
            }
        }
        entity_config = EntityConfig(**config)
        writer = DLTViewWriter(entity_config, mock_spark, mock_dbutils, mock_logger)
        
        with patch('writers.DLTViewWriter.dlt') as mock_dlt:
            mock_dlt.view.return_value = "mock_result"
            mock_dataframe = Mock(spec=DataFrame)
            
            writer._execute_declarative_operation(mock_dataframe)
            
            call_args = mock_dlt.view.call_args
            # Empty table_properties should not be included in view_config
            assert "table_properties" not in call_args[1] or not call_args[1]["table_properties"]
